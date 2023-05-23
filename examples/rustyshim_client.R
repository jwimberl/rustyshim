rustyshim_connect <- function(host = "localhost", username, password, port, scheme = "grpc+tcp", request_admin = FALSE) 
{
    prs <- reticulate::import("rustyshim_client")
    location <- paste0(scheme, "://", host, ":", port)
    prs$rustyshim_connect(location,username,password,request_admin=request_admin)
}

rustyshim_get_sql <- function(client, path) 
{
    reader <- client$get_sql(path)
    reader$read_all()
}

rustyshim_refresh_context <- function(client) 
{
    client$refresh_context()
}