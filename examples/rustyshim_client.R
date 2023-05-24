rustyshim_client <- R6::R6Class(
    "rustyshim_client",
    public = list(
        initialize = function(host, username, password, request_admin, port, scheme) {
            rc <- reticulate::import("rustyshim_client")
            private$pyclient <- rc$rustyshim_connect(host, username, password, request_admin, as.integer(port), scheme)
        },
        list_actions = function() {
            private$pyclient$list_actions()
        },
        refresh_context = function() {
            private$pyclient$refresh_context()
        },
        clear_expired_items = function() {
            private$pyclient$clear_expired_items()
        },
        get_sql = function(path) {
            reader <- private$pyclient$get_sql(path)
            reader$read_all()
        }
    ),
    private = list(
        pyclient = NULL
    )
)

rustyshim_connect <- function(host, username, password, request_admin=FALSE, port=50051, scheme = "grpc+tcp") 
{
    rustyshim_client$new(host, username, password, request_admin, port, scheme)
}
