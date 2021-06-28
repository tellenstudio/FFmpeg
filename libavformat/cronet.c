#include "avformat.h"
#include "network.h"
#include "url.h"
#include "libavutil/avstring.h"
#include "libavutil/fifo.h"
#include "libavutil/opt.h"
#include "libavutil/time.h"

#include <cronet_c.h>
#include <pthread.h>

struct CronetTask;

typedef struct CronetContext {
    const AVClass *class;
    int timeout;
    int seekable;
    int fifo_size;
    int max_fifo_size;
    AVFifoBuffer *fifo;
    pthread_mutex_t fifo_mutex;
    pthread_cond_t fifo_cond;
    char *location;     // Request uri.
    int64_t file_size;  // File total size.
    int64_t offset;     // Start offset in file of current request.
    int64_t read_pos;   // Current read offset in file of current request.
    int64_t write_pos;  // Current write offset in file of current request.
    Cronet_UrlRequestPtr request;
    Cronet_UrlRequestCallbackPtr callback;
    struct CronetTask *closing_task;
    int is_open;
} CronetContext;

#define OFFSET(x) offsetof(CronetContext, x)
#define D AV_OPT_FLAG_DECODING_PARAM
#define E AV_OPT_FLAG_ENCODING_PARAM
#define CRONET_DEFAULT_BUFFER_SIZE 524288
#define CRONET_MAX_BUFFER_SIZE  33554432
#define CRONET_HTTP_HEADER_SIZE 128

static const AVOption options[] = {
    { "timeout",        "Timeout in ms.",              OFFSET(timeout),        AV_OPT_TYPE_INT,     { .i64 = -1 },                           -1,  INT_MAX,      .flags = D|E },
    { "seekable",       "Whether seek is available.",  OFFSET(seekable),       AV_OPT_TYPE_BOOL,    { .i64 = -1 },                           -1,  1,            .flags = D|E },
    { "fifo_size",      "FIFO size.",                  OFFSET(fifo_size),      AV_OPT_TYPE_INT,     {.i64 = CRONET_DEFAULT_BUFFER_SIZE},      0,  INT_MAX,      .flags = D|E },
    { "max_fifo_size",  "Max FIFO size.",              OFFSET(max_fifo_size),  AV_OPT_TYPE_INT,     {.i64 = CRONET_MAX_BUFFER_SIZE},          0,  INT_MAX,      .flags = D|E },
    { NULL }
};

#define CRONET_CLASS(flavor)                        \
static const AVClass flavor ## _context_class = {   \
    .class_name = # flavor,                         \
    .item_name  = av_default_item_name,             \
    .option     = options,                          \
    .version    = LIBAVUTIL_VERSION_INT,            \
}

CRONET_CLASS(cronet);
CRONET_CLASS(cronets);

typedef enum CronetTaskType {
    kCronetTaskType_Runnable = 0,
    kCronetTaskType_FF_Open,
    kCronetTaskType_FF_Close,
    kCronetTaskType_FF_Reset,
    kCronetTaskType_FF_Range
} CronetTaskType;

typedef struct CronetTask {
    CronetTaskType      type;
    Cronet_RunnablePtr  runnable;
    URLContext          *url_context;
    char                *url;
    int64_t             start;
    int64_t             end;
    pthread_mutex_t     invoke_mutex;
    pthread_cond_t      invoke_cond;
    int                 invoke_returned;
    int                 invoke_return_value;
    struct CronetTask   *next;
} CronetTask;

typedef struct CronetTaskQueue {
    CronetTask          *head;
    CronetTask          *tail;
    int                 size;
} CronetTaskQueue;

typedef struct CronetRuntimeContext {
    Cronet_EnginePtr    engine;
    Cronet_ExecutorPtr  executor;
    pthread_t           executor_thread;
    pthread_mutex_t     executor_task_mutex;
    pthread_cond_t      executor_task_cond;
    CronetTaskQueue     executor_task_queue;
    int                 executor_stop_thread_loop;
#ifdef _WIN32
    Cronet_ComInitializerPtr com_initializer;
#endif
} CronetRuntimeContext;

// Constants.
const char *CRONET_FFMPEG_USER_AGENT = "CronetFFmpeg/1";

// Global cronet runtime context, not thread safe.
static CronetRuntimeContext *cronet_runtime_context = NULL;

// Pre-defination.
static void cronet_execute(Cronet_ExecutorPtr self, Cronet_RunnablePtr runnable);

static void cronet_uninit_task_queue(CronetTaskQueue *queue);

static void on_redirect_received(Cronet_UrlRequestCallbackPtr self,
                                 Cronet_UrlRequestPtr request,
                                 Cronet_UrlResponseInfoPtr info,
                                 Cronet_String newLocationUrl);

static void on_response_started(Cronet_UrlRequestCallbackPtr self,
                                Cronet_UrlRequestPtr request,
                                Cronet_UrlResponseInfoPtr info);

static void on_read_completed(Cronet_UrlRequestCallbackPtr self,
                              Cronet_UrlRequestPtr request,
                              Cronet_UrlResponseInfoPtr info,
                              Cronet_BufferPtr buffer,
                              uint64_t bytes_read);

static void on_succeeded(Cronet_UrlRequestCallbackPtr self,
                         Cronet_UrlRequestPtr request,
                         Cronet_UrlResponseInfoPtr info);

static void on_failed(Cronet_UrlRequestCallbackPtr self,
                      Cronet_UrlRequestPtr request,
                      Cronet_UrlResponseInfoPtr info,
                      Cronet_ErrorPtr error);

static void on_canceled(Cronet_UrlRequestCallbackPtr self,
                        Cronet_UrlRequestPtr request,
                        Cronet_UrlResponseInfoPtr info);

static void on_metrics_collected(Cronet_UrlRequestCallbackPtr self,
                                 Cronet_UrlRequestPtr request,
                                 Cronet_String metrics);

static void stop_read(CronetContext *s);

static CronetTask* create_task(CronetTaskType type) {
    CronetTask *task    = av_mallocz(sizeof(CronetTask));
    task->type          = type;

    pthread_mutex_init(&task->invoke_mutex, NULL);
    pthread_cond_init(&task->invoke_cond, NULL);
    return task;
}

static CronetTask* create_context_task(CronetTaskType type,
                                       URLContext *url_context) {
    CronetTask *task    = create_task(type);
    task->url_context   = url_context;
    return task;
}

static CronetTask* create_runnable_task(Cronet_RunnablePtr runnable) {
    CronetTask *task    = create_task(kCronetTaskType_Runnable);
    task->runnable      = runnable;
    return task;
}

static CronetTask* create_open_task(URLContext *url_context, const char *url) {
    CronetTask *task    = create_context_task(kCronetTaskType_FF_Open, url_context);
    task->url           = av_strdup(url);
    return task;
}

static CronetTask* create_close_task(URLContext *url_context) {
    return create_context_task(kCronetTaskType_FF_Close, url_context);
}

static CronetTask* create_reset_task(URLContext *url_context) {
    return create_context_task(kCronetTaskType_FF_Reset, url_context);
}

static CronetTask* create_range_task(URLContext *url_context,
                                     int64_t start,
                                     int64_t end) {
    CronetTask *task    = create_context_task(kCronetTaskType_FF_Range, url_context);
    task->start         = start;
    task->end           = end;
    return task;
}

static void destroy_task(CronetTask *task) {
    if (task == NULL) {
        return;
    }

    if (task->runnable != NULL) {
        Cronet_Runnable_Destroy(task->runnable);
    }

    if (task->url != NULL) {
        av_freep(&task->url);
    }

    pthread_mutex_destroy(&task->invoke_mutex);
    pthread_cond_destroy(&task->invoke_cond);

    av_free(task);
}

static void cronet_init_task_queue(CronetTaskQueue *queue) {
    if (queue == NULL) {
        return;
    }

    if (queue->head != NULL) {
        cronet_uninit_task_queue(queue);
    }

    queue->head = av_mallocz(sizeof(CronetTask));
    queue->tail = queue->head;
    queue->size = 0;
}

static void cronet_uninit_task_queue(CronetTaskQueue *queue) {
    CronetTask *task = NULL;
    if (queue == NULL) {
        return;
    }

    task = queue->head;
    while (task != NULL) {
        CronetTask *next = task->next;
        destroy_task(task);
        task = next;
    }

    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;
}

static void cronet_enqueue_task(CronetTaskQueue *queue,
                                CronetTask *task) {
    if (queue == NULL || queue->tail == NULL || task == NULL) {
        return;
    }

    queue->tail->next   = task;
    queue->tail         = task;

    ++queue->size;
}

static CronetTask* cronet_dequeue_task(CronetTaskQueue *queue) {
    CronetTask *task = NULL;
    if (queue == NULL || queue->head == NULL || queue->size == 0) {
        return NULL;
    }

    task = queue->head->next;
    if (task == NULL) {
        return NULL;
    }

    queue->head->next   = task->next;

    if (queue->tail == task) {
        queue->tail = queue->head;
    }

    --queue->size;

    return task;
}

// Invoke a task on task thread, will automatically 
// delete the task, DO NOT delete it manually.
static int invoke_task(CronetTask *task) {
    int posted = 0;
    int ret = AVERROR_UNKNOWN;

    if (task == NULL || cronet_runtime_context == NULL) {
        destroy_task(task);
        return AVERROR(EINVAL);
    }

    // Try to post to task queue.
    pthread_mutex_lock(&cronet_runtime_context->executor_task_mutex);
    if (!cronet_runtime_context->executor_stop_thread_loop) {
        cronet_enqueue_task(&cronet_runtime_context->executor_task_queue, task);
        pthread_cond_signal(&cronet_runtime_context->executor_task_cond);
        posted = 1;
    }
    pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);

    // Wait for return value.
    pthread_mutex_lock(&task->invoke_mutex);
    while (posted && !task->invoke_returned) {
        pthread_cond_wait(&task->invoke_cond,
                          &task->invoke_mutex);
    }
    pthread_mutex_unlock(&task->invoke_mutex);

    if (posted) {
        ret = task->invoke_return_value;
    }

    destroy_task(task);
    return ret;
}

static void post_return_value(CronetTask *task, int ret) {
    if (task == NULL || cronet_runtime_context == NULL) {
        return;
    }

    pthread_mutex_lock(&task->invoke_mutex);
    task->invoke_returned       = 1;
    task->invoke_return_value   = ret;
    pthread_cond_signal(&task->invoke_cond);
    pthread_mutex_unlock(&task->invoke_mutex);
}

static void process_runnable_task(CronetTask *task) {
    Cronet_RunnablePtr runnable = NULL;
    if (task == NULL) {
        return;
    }

    runnable = task->runnable;
    if (runnable != NULL) {
        Cronet_Runnable_Run(runnable);
    }

    destroy_task(task);
}

static void process_open_task(CronetTask *task) {
    URLContext *h               = NULL;
    CronetContext *s            = NULL;
    const char *uri             = NULL;
    char *url                   = NULL;
    int url_len                 = 0;
    const char *http_scheme     = "http://";
    const char *https_scheme    = "https://";
    const char *scheme          = NULL;
    const char *deli            = "://";
    char *p                     = NULL;
    char hostname[1024]         = {0};
    char protocol[1024]         = {0};
    char path[1024]             = {0};
    int port                    = 0;
    Cronet_RESULT result        = Cronet_RESULT_SUCCESS;
    Cronet_UrlRequestParamsPtr request_params = NULL;
    int ret = 0;

    do {
        if (cronet_runtime_context == NULL) {
            return;
        }

        if (cronet_runtime_context->engine == NULL ||
            cronet_runtime_context->executor == NULL) {
            ret = AVERROR_UNKNOWN;
            break;
        }

        if (task == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        h   = task->url_context;
        uri = task->url;

        if (h == NULL || h->priv_data == NULL || uri == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        s = h->priv_data;

        av_url_split(protocol, sizeof(protocol), NULL, 0, hostname, sizeof(hostname), &port, path, sizeof(path), uri);
        if (strncmp(protocol, "cronet", strlen(protocol)) == 0) {
            scheme = http_scheme;
        } else if (strncmp(protocol, "cronets", strlen(protocol)) == 0) {
            scheme = https_scheme;
        } else {
            ret = AVERROR_PROTOCOL_NOT_FOUND;
            break;
        }

        //Replace protocol with certain scheme.
        p       = strstr(uri, deli);
        p       += strlen(deli);
        url_len = strlen(scheme) + strlen(p) + 1;
        url     = (char*)av_mallocz(url_len);
        strcpy(url, scheme);
        strcat(url, p);

        if (s->seekable == 1) {
            h->is_streamed = 0;
        } else {
            h->is_streamed = 1;
        }

        s->file_size    = UINT64_MAX;
        s->offset       = 0;
        s->read_pos     = 0;
        s->write_pos    = 0;
        s->location     = av_strdup(url);
        s->is_open      = 0;

        // Setup fifo buffer.
        s->fifo = av_fifo_alloc(s->fifo_size);
        pthread_mutex_init(&s->fifo_mutex, NULL);
        pthread_cond_init(&s->fifo_cond, NULL);

        // Setup callback.
        s->callback = Cronet_UrlRequestCallback_CreateWith(on_redirect_received,
                                                           on_response_started,
                                                           on_read_completed,
                                                           on_succeeded,
                                                           on_failed,
                                                           on_canceled);
                                                           //on_metrics_collected);
        Cronet_UrlRequestCallback_SetClientContext(s->callback, h);

        // Setup request.
        s->request = Cronet_UrlRequest_Create();

        // Setup request param.
        request_params = Cronet_UrlRequestParams_Create();
        Cronet_UrlRequestParams_http_method_set(request_params, "GET");
        result = Cronet_UrlRequest_InitWithParams(s->request,
                                                  cronet_runtime_context->engine,
                                                  s->location,
                                                  request_params,
                                                  s->callback,
                                                  cronet_runtime_context->executor);
        if (result != Cronet_RESULT_SUCCESS) {
            av_log(h,
                   AV_LOG_ERROR,
                   "Cronet_UrlRequest_InitWithParams error %d.\n", result);
            ret = AVERROR_UNKNOWN;
            break;
        }

        // Starting request.
        result = Cronet_UrlRequest_Start(s->request);
        if (result != Cronet_RESULT_SUCCESS) {
            av_log(h,
                   AV_LOG_ERROR,
                   "Cronet_UrlRequest_Start error %d.\n", result);
            ret = AVERROR_UNKNOWN;
            break;
        }

        s->is_open = 1;
    } while (0);

    if (ret != 0) {
        if (s->request != NULL) {
            Cronet_UrlRequest_Destroy(s->request);
            s->request = NULL;
        }

        if (s->callback != NULL) {
            Cronet_UrlRequestCallback_Destroy(s->callback);
            s->callback = NULL;
        }

        if (s->fifo != NULL) {
            av_fifo_freep(&s->fifo);
            pthread_mutex_destroy(&s->fifo_mutex);
            pthread_cond_destroy(&s->fifo_cond);
        }
    }

    if (request_params != NULL) {
        Cronet_UrlRequestParams_Destroy(request_params);
    }

    if (url != NULL) {
        av_free(url);
    }

    post_return_value(task, ret);
}

static void process_close_task(CronetTask *task) {
    URLContext *h       = NULL;
    CronetContext *s    = NULL;
    int ret             = 0;
    int do_cancel       = 0;
    do {
        if (cronet_runtime_context == NULL) {
            return;
        }

        if (task == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        h = task->url_context;

        if (h == NULL || h->priv_data == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        s = h->priv_data;
        stop_read(s);

        if (s->request != NULL) {
            if (s->closing_task == NULL) {
                Cronet_UrlRequest_Cancel(s->request);
                s->closing_task = task;
                do_cancel = 1;
            } else {
                // Already closing, just return.
                break;
            }
        }

        if (s->fifo != NULL) {
            av_fifo_freep(&s->fifo);
            pthread_mutex_destroy(&s->fifo_mutex);
            pthread_cond_destroy(&s->fifo_cond);
        }

        s->file_size    = UINT64_MAX;
        s->offset       = 0;
        s->read_pos     = 0;
        s->write_pos    = 0;
        s->is_open      = 0;

        if (s->location != NULL) {
            av_freep(&s->location);
        }
    } while (0);

    if (!do_cancel) {
        post_return_value(task, ret);
    }
}

static void process_reset_task(CronetTask *task) {
    URLContext *h       = NULL;
    CronetContext *s    = NULL;
    int ret             = 0;
    int do_cancel       = 0;
    do {
        if (cronet_runtime_context == NULL) {
            return;
        }

        if (task == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        h = task->url_context;

        if (h == NULL || h->priv_data == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        s = h->priv_data;
        stop_read(s);

        if (s->fifo == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        if (s->request != NULL) {
            if (s->closing_task == NULL) {
                Cronet_UrlRequest_Cancel(s->request);
                s->closing_task = task;
                do_cancel = 1;
            } else {
                // Already closing, just return.
                break;
            }
        }

        s->read_pos     = 0;
        s->write_pos    = 0;
        s->is_open      = 0;

        av_fifo_reset(s->fifo);
    } while (0);

    if (!do_cancel) {
        post_return_value(task, ret);
    }
}

static void process_range_task(CronetTask *task) {
    URLContext *h           = NULL;
    CronetContext *s        = NULL;
    int64_t start           = 0;
    int64_t end             = 0;
    Cronet_RESULT result    = Cronet_RESULT_SUCCESS;
    Cronet_UrlRequestParamsPtr request_params = NULL;
    Cronet_HttpHeaderPtr header = NULL;
    char range_header[CRONET_HTTP_HEADER_SIZE] = {0};
    int ret = 0;

    do {
        if (cronet_runtime_context == NULL) {
            return;
        }

        if (cronet_runtime_context->engine == NULL ||
            cronet_runtime_context->executor == NULL) {
            ret = AVERROR_UNKNOWN;
            break;
        }

        if (task == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        h       = task->url_context;
        start   = task->start;
        end     = task->end;

        if (h == NULL || h->priv_data == NULL) {
            ret = AVERROR(EINVAL);
            break;
        }

        if (end >= start) {
            ret = AVERROR(EINVAL);
            break;
        }

        s = h->priv_data;

        // Setup callback.
        s->callback = Cronet_UrlRequestCallback_CreateWith(on_redirect_received,
                                                           on_response_started,
                                                           on_read_completed,
                                                           on_succeeded,
                                                           on_failed,
                                                           on_canceled);
                                                           //on_metrics_collected);
        Cronet_UrlRequestCallback_SetClientContext(s->callback, h);

        // Setup request.
        s->request = Cronet_UrlRequest_Create();

        // Setup request param.
        request_params = Cronet_UrlRequestParams_Create();

        // Set method.
        Cronet_UrlRequestParams_http_method_set(request_params, "GET");

        // Set range header.
        header = Cronet_HttpHeader_Create();
        Cronet_HttpHeader_name_set(header, "Range");
        if (end != -1) {
            av_strlcatf(range_header, sizeof(range_header), "bytes=%"PRIu64"-%"PRIu64, start, end);
        } else {
            av_strlcatf(range_header, sizeof(range_header), "bytes=%"PRIu64"-", start);
        }
        Cronet_HttpHeader_value_set(header, range_header);
        Cronet_UrlRequestParams_request_headers_add(request_params, header);

        result = Cronet_UrlRequest_InitWithParams(s->request,
                                                  cronet_runtime_context->engine,
                                                  s->location,
                                                  request_params,
                                                  s->callback,
                                                  cronet_runtime_context->executor);
        if (result != Cronet_RESULT_SUCCESS) {
            av_log(h,
                   AV_LOG_ERROR,
                   "Cronet_UrlRequest_InitWithParams error %d.\n", result);
            ret = AVERROR_UNKNOWN;
            break;
        }

        // Starting request.
        result = Cronet_UrlRequest_Start(s->request);
        if (result != Cronet_RESULT_SUCCESS) {
            av_log(h,
                   AV_LOG_ERROR,
                   "Cronet_UrlRequest_Start error %d.\n", result);
            ret = AVERROR_UNKNOWN;
            break;
        }

        s->read_pos     = start;
        s->write_pos    = start;
        s->is_open      = 1;
    } while (0);

    if (ret != 0) {
        if (s->request != NULL) {
            Cronet_UrlRequest_Destroy(s->request);
            s->request = NULL;
        }

        if (s->callback != NULL) {
            Cronet_UrlRequestCallback_Destroy(s->callback);
            s->callback = NULL;
        }
    }

    if (request_params != NULL) {
        Cronet_UrlRequestParams_Destroy(request_params);
    }

    if (header != NULL) {
        Cronet_HttpHeader_Destroy(header);
    }

    post_return_value(task, ret);
}

static void process_task(CronetTask *task) {
    if (task == NULL) {
        return;
    }

    switch (task->type) {
        case kCronetTaskType_Runnable:
            process_runnable_task(task);
            break;
        case kCronetTaskType_FF_Open:
            process_open_task(task);
            break;
        case kCronetTaskType_FF_Close:
            process_close_task(task);
            break;
        case kCronetTaskType_FF_Reset:
            process_reset_task(task);
            break;
        case kCronetTaskType_FF_Range:
            process_range_task(task);
            break;
        default:
            break;
    }
}

static void cronet_execute(Cronet_ExecutorPtr self, Cronet_RunnablePtr runnable) {
    if (runnable == NULL || cronet_runtime_context == NULL) {
        return;
    }

    pthread_mutex_lock(&cronet_runtime_context->executor_task_mutex);
    if (!cronet_runtime_context->executor_stop_thread_loop) {
        cronet_enqueue_task(&cronet_runtime_context->executor_task_queue,
                            create_runnable_task(runnable));
        pthread_cond_signal(&cronet_runtime_context->executor_task_cond);
    }
    pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);
}

static void *executor_thread_loop(void *arg) {
    if (cronet_runtime_context == NULL) {
        return NULL;
    }

    // Process task in task queue.
    while (1) {
        CronetTask *task = NULL;

        // Wait for a task to run or stop signal.
        pthread_mutex_lock(&cronet_runtime_context->executor_task_mutex);
        while (cronet_runtime_context->executor_task_queue.size == 0 &&
               !cronet_runtime_context->executor_stop_thread_loop) {
            pthread_cond_wait(&cronet_runtime_context->executor_task_cond,
                              &cronet_runtime_context->executor_task_mutex);
        }

        if (cronet_runtime_context->executor_stop_thread_loop) {
            pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);
            break;
        }

        if (cronet_runtime_context->executor_task_queue.size == 0) {
            pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);
            continue;
        }

        task = cronet_dequeue_task(&cronet_runtime_context->executor_task_queue);
        pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);

        process_task(task);
    }

    // Uninitialize task queue.
    pthread_mutex_lock(&cronet_runtime_context->executor_task_mutex);
    cronet_uninit_task_queue(&cronet_runtime_context->executor_task_queue);
    pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);

    return NULL;
}

// Create cronet engine.
static Cronet_EnginePtr create_cronet_engine() {
    Cronet_RESULT result                    = Cronet_RESULT_SUCCESS;
    Cronet_EnginePtr cronet_engine          = Cronet_Engine_Create();
    Cronet_EngineParamsPtr engine_params    = Cronet_EngineParams_Create();
    Cronet_EngineParams_user_agent_set(engine_params, CRONET_FFMPEG_USER_AGENT);

    // Enable HTTP2.
    Cronet_EngineParams_enable_http2_set(engine_params, 1);

    // Enable QUIC.
    Cronet_EngineParams_enable_quic_set(engine_params,  1);

    // Start cronet engine.
    result = Cronet_Engine_StartWithParams(cronet_engine, engine_params);
    if (result != Cronet_RESULT_SUCCESS) {
        av_log(NULL,
               AV_LOG_ERROR,
               "Cronet_Engine_StartWithParams error %d.\n", result);
        Cronet_Engine_Destroy(cronet_engine);
        cronet_engine = NULL;
    }

    Cronet_EngineParams_Destroy(engine_params);
    return cronet_engine;
}

int av_format_cronet_init() {
    int ret = 0;
    do {
        // Already initialized.
        if (cronet_runtime_context != NULL) {
            break;
        }

        // Alloc CronetRuntimeContext.
        cronet_runtime_context = av_mallocz(sizeof(CronetRuntimeContext));
        cronet_runtime_context->executor_stop_thread_loop   = false;

        //Setup Cronet_Engine.
        cronet_runtime_context->engine = create_cronet_engine();
        if (cronet_runtime_context->engine == NULL) {
            av_log(NULL, AV_LOG_ERROR, "create_cronet_engine fail.\n");
            ret = AVERROR(EINVAL);
            break;
        }

        // Setup Cronet_Executor.
        cronet_runtime_context->executor = Cronet_Executor_CreateWith(cronet_execute);
        Cronet_Executor_SetClientContext(cronet_runtime_context->executor,
                                         cronet_runtime_context);

        // Initialize executor task mutex and condition variable.
        pthread_mutex_init(&cronet_runtime_context->executor_task_mutex, NULL);
        pthread_cond_init(&cronet_runtime_context->executor_task_cond, NULL);

        // Initialize task queue.
        cronet_init_task_queue(&cronet_runtime_context->executor_task_queue);

        // Create executor thread.
        ret = pthread_create(&cronet_runtime_context->executor_thread,
                             NULL,
                             executor_thread_loop,
                             NULL);
        if (ret) {
            av_log(NULL, AV_LOG_ERROR, "Cronet pthread_create fail.\n");
            cronet_uninit_task_queue(&cronet_runtime_context->executor_task_queue);
            pthread_mutex_destroy(&cronet_runtime_context->executor_task_mutex);
            pthread_cond_destroy(&cronet_runtime_context->executor_task_cond);
            ret = AVERROR(ret);
            break;
        }
    } while (0);

    if (ret != 0 && cronet_runtime_context != NULL) {
        if (cronet_runtime_context->executor != NULL) {
            Cronet_Executor_Destroy(cronet_runtime_context->executor);
            cronet_runtime_context->executor = NULL;
        }

        if (cronet_runtime_context->engine != NULL) {
            Cronet_Engine_Shutdown(cronet_runtime_context->engine);
            Cronet_Engine_Destroy(cronet_runtime_context->engine);
            cronet_runtime_context->engine = NULL;
        }

        av_freep(&cronet_runtime_context);
    }

    return ret;
}

void av_format_cronet_uninit(void) {
    // Not intialized.
    if (cronet_runtime_context == NULL) {
        return;
    }

    // Stop executor thread.
    pthread_mutex_lock(&cronet_runtime_context->executor_task_mutex);
    cronet_runtime_context->executor_stop_thread_loop = 1;
    pthread_cond_signal(&cronet_runtime_context->executor_task_cond);
    pthread_mutex_unlock(&cronet_runtime_context->executor_task_mutex);
    pthread_join(cronet_runtime_context->executor_thread, NULL);

    // Destroy executor task mutex and condition variable.
    pthread_mutex_destroy(&cronet_runtime_context->executor_task_mutex);
    pthread_cond_destroy(&cronet_runtime_context->executor_task_cond);

    // Destroy Cronet_Executor 
    if (cronet_runtime_context->executor != NULL) {
        Cronet_Executor_Destroy(cronet_runtime_context->executor);
        cronet_runtime_context->executor = NULL;
    }

    // Destroy Cronet_Engine.
    if (cronet_runtime_context->engine != NULL) {
        Cronet_Engine_Shutdown(cronet_runtime_context->engine);
        Cronet_Engine_Destroy(cronet_runtime_context->engine);
        cronet_runtime_context->engine = NULL;
    }

    // Destroy CronetRuntimeContext.
    av_freep(&cronet_runtime_context);
}

#ifdef _WIN32
void av_format_cronet_init_com(void) {
    if (cronet_runtime_context != NULL) {
        cronet_runtime_context->com_initializer = Cronet_ComInitializer_Create();
    }
}

void av_format_cronet_uninit_com(void) {
     if (cronet_runtime_context != NULL &&
         cronet_runtime_context->com_initializer != NULL) {
        Cronet_ComInitializer_Destroy(cronet_runtime_context->com_initializer);
        cronet_runtime_context->com_initializer = NULL;
    }
}
#endif  // #ifdef _WIN32

static int process_headers(URLContext *h, Cronet_UrlResponseInfoPtr info) {
    uint32_t header_num = 0;
    const char *slash   = NULL;
    const char *p       = NULL;
    CronetContext *s    = NULL;

    if (h == NULL || info == NULL) {
        return 0;
    }

    s = h->priv_data;
    if (s == NULL) {
        return 0;
    }

    if (!s->is_open) {
        return 0;
    }

    header_num = Cronet_UrlResponseInfo_all_headers_list_size(info);
    for (uint32_t i = 0; i < header_num; ++i) {
        Cronet_HttpHeaderPtr header = Cronet_UrlResponseInfo_all_headers_list_at(info, i);
        Cronet_String key           = Cronet_HttpHeader_name_get(header);
        Cronet_String value         = Cronet_HttpHeader_value_get(header);

        if (!av_strcasecmp(key, "Content-Range")) {
            p = value;
            if (!strncmp(p, "bytes ", 6)) {
                p += 6;
                s->offset       = strtoull(p, NULL, 10);
                s->read_pos     = s->offset;
                if ((slash = strchr(p, '/')) && strlen(slash) > 0) {
                    s->file_size = strtoull(slash + 1, NULL, 10);
                }
            }
            if (s->seekable == -1 && s->file_size != UINT64_MAX) {
                h->is_streamed = 0;
            }
        } else if (!av_strcasecmp(key, "Content-Length") && s->file_size == UINT64_MAX) {
            s->file_size = strtoull(value, NULL, 10);
        } else if (!av_strcasecmp(key, "Accept-Ranges") &&
                   !strncmp(value, "bytes", 5) &&
                   s->seekable == -1) {
            h->is_streamed = 0;
        }
    }
    return 1;
}

static void stop_read(CronetContext *s) {
    if (s != NULL) {
        pthread_mutex_lock(&s->fifo_mutex);
        s->is_open = 0;
        pthread_cond_signal(&s->fifo_cond);
        pthread_mutex_unlock(&s->fifo_mutex);
    }
}

// Implementation of Cronet_UrlRequestCallback methods.
static void on_redirect_received(Cronet_UrlRequestCallbackPtr self,
                                 Cronet_UrlRequestPtr request,
                                 Cronet_UrlResponseInfoPtr info,
                                 Cronet_String newLocationUrl) {
    if (request != NULL) {
        Cronet_UrlRequest_FollowRedirect(request);
    }
}

static void on_response_started(Cronet_UrlRequestCallbackPtr self,
                                Cronet_UrlRequestPtr request,
                                Cronet_UrlResponseInfoPtr info) {
    Cronet_BufferPtr buffer = NULL;

    // Process headers.
    URLContext *h = (URLContext *)Cronet_UrlRequestCallback_GetClientContext(self);
    if (!process_headers(h, info)) {
        return;
    }

    // Don't call Cronet_Buffer_Destroy manually, it will be
    // deleted automatically.
    buffer = Cronet_Buffer_Create();
    Cronet_Buffer_InitWithAlloc(buffer, CRONET_DEFAULT_BUFFER_SIZE);
    Cronet_UrlRequest_Read(request, buffer);

    Cronet_String url = Cronet_UrlResponseInfo_url_get(info);
    int32_t http_status_code = Cronet_UrlResponseInfo_http_status_code_get(info);
    Cronet_String http_status_text = Cronet_UrlResponseInfo_http_status_text_get(info);
    Cronet_String protocol = Cronet_UrlResponseInfo_negotiated_protocol_get(info);

    av_log(h, AV_LOG_INFO, "Cronet on_response_started.\n url=%s, %d %s, protocol=%s",
    url, http_status_code, http_status_text, protocol);
}

static void on_read_completed(Cronet_UrlRequestCallbackPtr self,
                              Cronet_UrlRequestPtr request,
                              Cronet_UrlResponseInfoPtr info,
                              Cronet_BufferPtr buffer,
                              uint64_t bytes_read) {
    char *buf           = NULL;
    int size            = 0;
    int capacity        = 0;
    int write_len       = 0;
    URLContext *h       = NULL;
    CronetContext *s    = NULL;

    h = (URLContext *)Cronet_UrlRequestCallback_GetClientContext(self);
    if (h == NULL) {
        return;
    }

    s = h->priv_data;
    if (s == NULL) {
        return;
    }

    if (!s->is_open) {
        return;
    }

    buf     = (char*)Cronet_Buffer_GetData(buffer);
    size    = (int)bytes_read;

    av_log(h, AV_LOG_INFO, "Cronet on_read_completed. bytes_read=%d", size);

    pthread_mutex_lock(&s->fifo_mutex);
    do {
        if (s->fifo == NULL) {
            break;
        }

        // Space not sufficient.
        if(av_fifo_space(s->fifo) < size) {
            capacity = av_fifo_size(s->fifo) + av_fifo_space(s->fifo);
            if (capacity >= s->max_fifo_size) {
                av_log(h,
                       AV_LOG_ERROR,
                       "FIFO overflow, capacity %d, free space %d, input %d.\n",
                       capacity,
                       av_fifo_space(s->fifo),
                       size);
                break;
            } else {
                av_fifo_grow(s->fifo, size);
            }
        }

        // Try to write.
        write_len = av_fifo_generic_write(s->fifo, (uint8_t *)buf, size, NULL);
        if (write_len != size) {
            // Should not happen.
            av_log(h,
                   AV_LOG_WARNING,
                   "FIFO written %d, expect %d.\n",
                   write_len,
                   size);
        }

        // Notify read thread.
        pthread_cond_signal(&s->fifo_cond);

        // Update write_pos.
        s->write_pos += write_len;

        // Continue reading.
        if (s->file_size == UINT64_MAX || s->write_pos <= s->file_size) {
            Cronet_UrlRequest_Read(request, buffer);
        }
    } while (0);
    pthread_mutex_unlock(&s->fifo_mutex);
}

static void on_succeeded(Cronet_UrlRequestCallbackPtr self,
                         Cronet_UrlRequestPtr request,
                         Cronet_UrlResponseInfoPtr info) {
    URLContext *h       = NULL;
    CronetContext *s    = NULL;
    do {
        h = (URLContext *)Cronet_UrlRequestCallback_GetClientContext(self);
        if (h == NULL) {
            break;
        }

        s = h->priv_data;
        if (s == NULL) {
            break;
        }

        if (s->request != NULL) {
            Cronet_UrlRequest_Destroy(s->request);
            s->request = NULL;
        }

        if (s->callback != NULL) {
            Cronet_UrlRequestCallback_Destroy(s->callback);
            s->callback = NULL;
        }

        stop_read(s);
    } while (0);

    av_log(h, AV_LOG_INFO, "Cronet on_succeeded.");
}

static void on_failed(Cronet_UrlRequestCallbackPtr self,
                      Cronet_UrlRequestPtr request,
                      Cronet_UrlResponseInfoPtr info,
                      Cronet_ErrorPtr error) {
    URLContext *h       = NULL;
    CronetContext *s    = NULL;
    do {
        h = (URLContext *)Cronet_UrlRequestCallback_GetClientContext(self);
        if (h == NULL) {
            break;
        }

        s = h->priv_data;
        if (s == NULL) {
            break;
        }

        if (s->request != NULL) {
            Cronet_UrlRequest_Destroy(s->request);
            s->request = NULL;
        }

        if (s->callback != NULL) {
            Cronet_UrlRequestCallback_Destroy(s->callback);
            s->callback = NULL;
        }

        stop_read(s);
    } while (0);

    av_log(h,
           AV_LOG_ERROR,
           "Cronet Request error %d %s.\n",
           Cronet_Error_error_code_get(error),
           Cronet_Error_message_get(error));
}

static void on_canceled(Cronet_UrlRequestCallbackPtr self,
                        Cronet_UrlRequestPtr request,
                        Cronet_UrlResponseInfoPtr info) {
    URLContext *h       = NULL;
    CronetContext *s    = NULL;
    do {
        h = (URLContext *)Cronet_UrlRequestCallback_GetClientContext(self);
        if (h == NULL) {
            break;
        }

        s = h->priv_data;
        if (s == NULL) {
            break;
        }

        if (s->request != NULL) {
            Cronet_UrlRequest_Destroy(s->request);
            s->request = NULL;
        }

        if (s->callback != NULL) {
            Cronet_UrlRequestCallback_Destroy(s->callback);
            s->callback = NULL;
        }

        s->is_open = false;
        post_return_value(s->closing_task, 0);
        s->closing_task = NULL;
    } while (0);
}

static void on_metrics_collected(Cronet_UrlRequestCallbackPtr self,
                                 Cronet_UrlRequestPtr request,
                                 Cronet_String metrics) {
    // Do nothing
    //av_log(NULL, AV_LOG_INFO, "on_metrics_collected %s.\n", metrics);
}

static int cronet_open(URLContext *h, const char *uri, int flags) {
    av_log(h, AV_LOG_INFO, "cronet_open uri=%s.\n", uri);

    int ret = 0;
    if (cronet_runtime_context == NULL) {
        ret = AVERROR_UNKNOWN;
    } else {
        ret = invoke_task(create_open_task(h, uri));
    }
    return ret;
}

static int cronet_close(URLContext *h) {
    int ret = 0;
    if (cronet_runtime_context == NULL) {
        ret = AVERROR_UNKNOWN;
    } else {
        ret = invoke_task(create_close_task(h));
    }
    return ret;
}

static int cronet_read(URLContext *h, uint8_t *buf, int size) {
    int ret             = 0;
    int avail           = 0;
    CronetContext *s    = h->priv_data;

    if (s->fifo == NULL) {
        return AVERROR_UNKNOWN;
    }

    pthread_mutex_lock(&s->fifo_mutex);
    do {
        if (s->file_size != UINT64_MAX && s->read_pos >= s->file_size) {
            ret = AVERROR_EOF;
            break;
        }

        avail = av_fifo_size(s->fifo);
        if (avail > 0) {
            // If some data available, return immediately.
            ret = FFMIN(avail, size);
            av_fifo_generic_read(s->fifo, buf, ret, NULL);

            //Update read_pos.
            s->read_pos += ret;
            av_log(h, AV_LOG_DEBUG, "cronet_read file_size=%lld read_pos=%lld ret=%lld. \n", s->file_size, s->read_pos, ret);
            break;
        }

        if (s->is_open == 0) {
            ret = AVERROR_EOF;
            break;
        }

        // Otherwise, wait for data.
        if (s->timeout == -1) {
            pthread_cond_wait(&s->fifo_cond, &s->fifo_mutex);
        } else {
            int64_t t = av_gettime() + s->timeout * 1000;
            struct timespec tv = { .tv_sec  =  t / 1000000,
                                   .tv_nsec = (t % 1000000) * 1000 };
            pthread_cond_timedwait(&s->fifo_cond, &s->fifo_mutex, &tv);
        }
        // Continue to read in next loop after got notified.
        continue;
    } while (1);

    pthread_mutex_unlock(&s->fifo_mutex);
    return ret;
}

static int cronet_write(URLContext *h, const uint8_t *buf, int size) {
    // Not implemented yet.
    return AVERROR(ENOSYS);
}

static int request_range(URLContext *h, int64_t start, int64_t end) {
    int ret = -1;
    do {
        ret = invoke_task(create_reset_task(h));
        if (ret != 0) {
            break;
        }

        ret = invoke_task(create_range_task(h, start, end));
    } while (0);
    return ret;
}

static int64_t cronet_seek(URLContext *h, int64_t off, int whence) {
    int64_t ret         = -1;
    int seek_from_net   = 0;
    CronetContext *s = h->priv_data;
    pthread_mutex_lock(&s->fifo_mutex);
    do {
        if (s->fifo == NULL) {
            ret = AVERROR_UNKNOWN;
            break;
        }

        if (whence == AVSEEK_SIZE) {
            ret = s->file_size;
            break;
        }

        if (((whence == SEEK_CUR && off == 0) || (whence == SEEK_SET && off == s->read_pos))) {
            ret = s->read_pos;
            break;
        }

        if ((s->file_size == UINT64_MAX && whence == SEEK_END)) {
            ret = AVERROR(ENOSYS);
            break;
        }

        if (whence == SEEK_CUR) {
            off += s->read_pos;
        } else if (whence == SEEK_END) {
            off += s->file_size;
        } else if (whence != SEEK_SET) {
            ret = AVERROR(EINVAL);
            break;
        }

        if (off < 0) {
            ret = AVERROR(EINVAL);
            break;
        }

        if (off && h->is_streamed) {
            ret = AVERROR(ENOSYS);
            break;
        }

        ret = off;

        // Check seek from buffer or net.
        if (off > s->read_pos && off < s->write_pos) {
            av_fifo_drain(s->fifo, off - s->read_pos);
            s->read_pos = off;
        } else if (off < s->read_pos || off >= s->write_pos) {
            seek_from_net = 1;
            break;
        }
    } while (0);
    pthread_mutex_unlock(&s->fifo_mutex);

    if (seek_from_net && request_range(h, off, -1) != 0) {
        ret = AVERROR(EINVAL);
    }

    //av_log(h, AV_LOG_INFO, "seek return %lld.\n", ret);
    return ret;
}

const URLProtocol ff_cronet_protocol = {
    .name                = "cronet",
    .url_open            = cronet_open,
    .url_close           = cronet_close,
    .url_read            = cronet_read,
    .url_write           = cronet_write,
    .url_seek            = cronet_seek,
    .priv_data_size      = sizeof(CronetContext),
    .priv_data_class     = &cronet_context_class,
    .flags               = URL_PROTOCOL_FLAG_NETWORK,
    .default_whitelist   = "cronet,cronets"
};

const URLProtocol ff_cronets_protocol = {
    .name                = "cronets",
    .url_open            = cronet_open,
    .url_close           = cronet_close,
    .url_read            = cronet_read,
    .url_write           = cronet_write,
    .url_seek            = cronet_seek,
    .priv_data_size      = sizeof(CronetContext),
    .priv_data_class     = &cronets_context_class,
    .flags               = URL_PROTOCOL_FLAG_NETWORK,
    .default_whitelist   = "cronet,cronets"
};
