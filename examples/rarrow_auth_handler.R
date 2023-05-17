rustyshim_connect <- function(host = "localhost", username, password, port, scheme = "grpc+tcp") 
{
    prs <- reticulate::import("pyarrow_auth_handler")
    location <- paste0(scheme, "://", host, ":", port)
    prs$rustyshim_connect(location,username, password)
}

rustyshim_get_sql <- function(client, path) 
{
    reader <- client$get_sql(path)
    reader$read_all()
}