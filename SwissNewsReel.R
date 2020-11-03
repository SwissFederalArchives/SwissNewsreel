library(httr)
library(jsonlite)
library(data.table)
library(stringr)
library(ggplot2)
library(magrittr)
library(svglite)
library(foreach)
library(doParallel)
corenum <- detectCores()
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# The request function ----
makerequest <- function(skip,take,decade) {
  jsonrequest <- paste0('{
    "query":{
      "searchGroups":[
        {"searchFields":[
          {"key":"referenceCode","value":"J2.143#1996/386*"},
          {"key": "creationPeriod","value": "',decade,'-',decade+10,'"}
        ],"fieldOperator":1}
      ],
      "groupOperator":1
    },
    "paging":{"skip":', skip ,',"take":', take ,',"orderBy":"","sortOrder":""},
    "facetsFilters": [
      {
        "filters": ["level:\\"Dokument\\""],
        "facet": "level"
      },
      {
        "filters": [
          "aggregationFields.bestand:\\"Stiftung Schweizer Filmwochenschau (1942-1975)\\""
        ],
        "facet": "aggregationFields.bestand"
      }
    ]
  }')
  POST(
    url,
    body = minify(jsonrequest), 
    encode = "raw",
    content_type_json()
  )
}

url <- "https://www.recherche.bar.admin.ch/recherche/api/v1/entities/Search"

# test <- makerequest(10000,1) %>% content()
# test$entities$items[[1]]$creationPeriod$text

# Fetch all data by bunches of 100 ----
stepstarts <- seq(1, ceiling(totalpages/100)*100, by = 100)
decadecounts <- data.table(decade = seq(1940,1970,by=10))
decadecounts[,count:= sapply(decade, function(x){
    res <- makerequest(1,1,x)
    fws <- content(res)
    fws$entities$paging$total
})]
cl <- parallel::makeCluster(corenum) 
doParallel::registerDoParallel(cl)
fws.datatable <- foreach(
  decade=decadecounts$decade,
  count=decadecounts$count,
  .packages = c("jsonlite","httr","data.table","magrittr"),
  .verbose = TRUE
  ) %:%
    foreach(
      i=seq(1, ceiling(count/100)*100, by = 100), 
      .combine=function(a,b)rbindlist(list(a,b))
    ) %dopar% {
      res <- makerequest(i,100,decade)
      fws <- content(res)
      data.table(
        refCode = sapply(fws$entities$items,function(x) return(x$archiveRecordId)),
        archiveID = sapply(fws$entities$items,function(x) return(x$referenceCode)),
        date = sapply(fws$entities$items,function(x) return(x$creationPeriod$text)),
        title = sapply(fws$entities$items,function(x) return(x$title)),
        dauer = sapply(fws$entities$items,function(x) return(x$customFields$format %>% unlist)),
        url = sapply(fws$entities$items,function(x) return(x$customFields$digitaleVersion %>% unlist %>% .["url"])),
        thema = sapply(fws$entities$items, function(x) x$customFields$thema %>% unlist)
      ) 
} %>% rbindlist(fill=TRUE)

# save(fws.datatable,file="FilmWochenSchau.RData")

# Extract variables with RegEx ----
# load("FilmWochenSchau.RData") 

# First, clean the data. Some columns could have been turned to lists due to API reply inconsistency
unlistColumn <- function(column) {
  sapply(column, function(x) {
    if (!is.null(unlist(x))) {return(unlist(x))} else return(NA)
  })
}
fws.datatable$refCode <- unlistColumn(fws.datatable$refCode)
fws.datatable$archiveID <- unlistColumn(fws.datatable$archiveID)
fws.datatable$date <- unlistColumn(fws.datatable$date)
fws.datatable$title <- unlistColumn(fws.datatable$title)
fws.datatable$dauer <- unlistColumn(fws.datatable$dauer)
fws.datatable$url <- unlistColumn(fws.datatable$url)
fws.datatable$thema <- unlistColumn(fws.datatable$thema)

# Filter duplicates
fws.datatable <- unique(fws.datatable, by="refCode") 

# Then apply RegEx
fws.datatable[,thema_orte := sapply(thema, function(x) {str_match(x, "Orte:[\\r\\n ]{1,3}([^\\r\\n]*)") %>% .[2]})]
fws.datatable[,thema_schlagworte := sapply(thema, function(x) {str_match(x, "Schlagworte:[\\r\\n ]{1,3}([^\\r\\n]*)") %>% .[2]})]
fws.datatable[,dauer_dauer := sapply(dauer, function(x) {str_match(x, "Dauer: ([0-9:]*)") %>% .[2]})]
fws.datatable[,dauer_seconds := sapply(dauer_dauer, function(x) {
  ifelse(
    str_count(x,":")>1,
    as.difftime(x, format = "%H:%M:%S", units = "secs") %>% strtoi,
    as.difftime(x, format = "%M:%S", units = "secs") %>% strtoi
  )
})]
fws.datatable[,date:=as.Date(date,"%d.%m.%Y")]

# Visualisation ----
emissions_par_date <- fws.datatable[, .N, by=date][, c("date","count","year","month", "N") := .(date,N,format(date,"%Y"),as.integer(format(date,"%m")), NULL)]
emissions_par_month <- emissions_par_date[, .(count=sum(count)), by=.(month,year)]
ggplot(emissions_par_month) +
  geom_col(aes(x=month,y=count),colour="white", fill="darkred", size=0.1) + 
  scale_x_continuous(
    breaks=c(3,6,9,12),
    minor_breaks = c(1,2,4,5,7,8,10,11)
  ) +
  coord_polar(start=pi/12) +
  facet_wrap(~year,ncol=10) +
  labs(title = "Ciné-Journal suisse",
       subtitle = "Nombre d'émissions diffusées par année et mois",
       caption = "Source: Archives fédérales suisses", 
       x = NULL, y = NULL) 
ggsave("cinejournal.svg",width=15,height=8)


# Fetch extra data from individual records ----
getDescription <- function(id) {
  resget <- GET(
    paste0("https://www.recherche.bar.admin.ch/recherche/api/v1/entities/",id), 
    encode = "json",
    content_type_json()
  )
  content(resget)
}

sequence <- c(seq(1, ceiling(fws.datatable%>%nrow/5000)*5000, by = 5000),fws.datatable %>% nrow)
for (i in 1:(sequence%>%length-1)){
  cl <- parallel::makeCluster(corenum) 
  doParallel::registerDoParallel(cl)
  fws.datatable[sequence[i]:sequence[i+1], darin := foreach(
    i = refCode,
    .export = "getDescription",
    .packages = c("jsonlite","httr","magrittr"),
    .verbose = TRUE
  ) %dopar% {
    x = getDescription(i)
    x[["withinInfo"]] %>% unlist
  }
  ]
  stopCluster(cl)
}
fws.datatable[,description_genre := sapply(darin, function(x) {str_match(x, "Genre:[\\r\\n ]{1,2}([^\\r\\n]*)") %>% .[2]})]
fws.datatable[,description_inhaltsangabe := sapply(darin, function(x) {str_match(x, "Inhaltsangabe:[\\r\\n ]{1,2}([^\\r\\n]*)") %>% .[2]})]
fws.datatable[,description_inhaltsangabe_ort := sapply(description_inhaltsangabe, function(x) {str_match(x, "^([^:]*)") %>% .[2]})]
ggplot(fws.datatable[,.N,by=description_genre]) + geom_col(aes(x=description_genre,y=N)) + coord_flip()
ggsave("cinejournal_genre.svg",width=9,height=4)
