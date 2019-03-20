f1 = function(e) 
{
  'x' %in% ls(envir=get(e,envir = .GlobalEnv))
}

nametree <- function(X, prefix = "")
  if( is.list(X) )
    for( i in seq_along(X) ) { 
      cat( prefix, names(X)[i], "\n", sep="" )
      nametree(X[[i]], paste0(prefix, "  "))
    }

