##################################
# 1. CARGA DE DATOS DESDE FUENTE #
##################################

rm(list = ls())

file_log <- paste0('Logs/',Sys.Date(),'_carga_datos.txt')
write("CARGA DE DATOS DESDE FUENTE",
      file = file_log)
write("---------------------------",
      file = file_log,
      append = T)

#CARGA DE LIBRERIAS
###################

library(RODBC)
library(tidyverse)
library(lubridate)

write(paste0('Carga de librerias','\t',Sys.time()),
      file = file_log,
      append = T)

###################

#IMPORTACION VISTAS DESDE DWH
#############################

#Conexion con DWH
driver <- '{SQL Server}'
server <- '192.168.101.76'
database <- 'MT_DW'
uid <- 'bi_reader'
pwd <- '@b1r3@d3r'
conn_text <- paste0('driver=',driver,';',
                    'server=',server,';',
                    'database=',database,';',
                    'uid=',uid,';',
                    'pwd=',pwd)
conn <- odbcDriverConnect(conn_text)

#Ocupacion Barcos
query_text <- "SELECT * FROM vw_fac_venta WHERE PyGPeriodoValor >= 2017"
df_ocupbarcos01 <- conn %>%
  sqlQuery(query_text) %>%
  as_tibble()

#Ocupacion Hoteles
query_text <- 'SELECT * FROM vw_fac_alojamiento WHERE YEAR(PyGFechaValor) >= 2017'
df_ocuphoteles01 <- conn %>%
  sqlQuery(query_text) %>%
  as_tibble()

#Cierre de conexion
odbcClose(conn)

write(paste0('Importacion vistas desde dwh','\t',Sys.time()),
      file = file_log,
      append = T)

#############################

#IMPORTACION DE CATALOGOS PLANOS
################################

#Capacidad productos
df_capacidad <- read_csv(file = 'Catalogos/capacidad.csv',
                         col_names = T) %>%
  as_tibble()

write(paste0('Importacion de catalogos planos','\t',Sys.time()),
      file = file_log,
      append = T)

################################

#PROCESAMIENTO INICIAL DE TABLAS
################################

#Barcos
cols_sel <- c(
  "TourDuracion","TourCombinado","TourCalcProfit","TourTieneMenores",
  "SrvProducto","SrvSalida","SrvItinerarioDestino","SrvInicio",
  "PrdFechaCreacion",
  "VtaValor","VtaPax",
  "PaxResidencia","PaxNacional"
)
df_ocupbarcos02 <- df_ocupbarcos01 %>%
  filter(TourGrupoEstado == 'Venta') %>%
  filter(SrvGrupoProducto == 'BPropios') %>%
  filter(SrvSubFamilia %in% c('ppto','aux','viajero','01. Alojamiento')) %>%
  filter(TourGrupoTipo %in% c('Comercial Ext','Comercial Int')) %>% 
  filter(TourCharter == 'No') %>%
  filter(BITipoInfo == 'Real') %>%
  filter(TourFuturo == 'No') %>%
  filter(!TourCalcProfit %in% c('CAPACITY','RRP')) %>%
  filter(!str_detect(SrvServicio, '^CNX')) %>%
  filter(!PyGPeriodoValor %in% c(2020,2021)) %>%
  select(all_of(cols_sel))

#Hoteles
cols_sel <- c(
  "ResFecha","ResCalcProfit","ResPaisResidencia","ResTieneVentaRetail",
  "SrvProducto",
  "PrdFechaCreacion",
  "VtaValor","VtaRN"
)
df_ocuphoteles02 <- df_ocuphoteles01 %>%
  filter(ResVentaSegura == 'Si') %>%
  filter(ResFutura == 'No') %>%
  filter(str_detect(PyGVersionPeriodo,'^R-')) %>%
  filter(SrvTipoServicio == 'HOTEL') %>%
  filter(ResTipo != 'Capacidad') %>%
  filter(!PyGPeriodoValor %in% c(2020,2021)) %>%
  select(all_of(cols_sel)) %>%
  mutate(ResNacional = if_else(ResPaisResidencia == 'Ecuador',
                               'Si',
                               'No'),
         .after = ResPaisResidencia)

write(paste0('Procesamiento inicial de tablas','\t',Sys.time()),
      file = file_log,
      append = T)

################################

#RESULTADOS GUARDADOS
#####################

df_barcos <- df_ocupbarcos02
df_hoteles <- df_ocuphoteles02

save(df_barcos,
     df_hoteles,
     df_capacidad,
     file = 'RDAs/carga_datos.rda')

write(paste0('Resultados guardados','\t',Sys.time()),
      file = file_log,
      append = T)

#####################

write("---------------------------",
      file = file_log,
      append = T)

rm(list = ls())
