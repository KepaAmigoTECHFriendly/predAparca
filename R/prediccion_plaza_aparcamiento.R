#' @title Predicción del estado de un aparcamiento las proximas horas
#'
#' @description Modelo KNN
#'
#' @return json
#'
#' @examples  prediccion_plaza_aparcamiento()
#'
#' @import httr
#' jsonlite
#' dplyr
#' lubridate
#' ordinal
#' caret
#' arm
#' class
#' gmodels
#'
#' @export

prediccion_plaza_aparcamiento<- function(){


  #----------------------------------------------------------------------------------------------------------------------------------------
  # VARIABLES INICIO
  #----------------------------------------------------------------------------------------------------------------------------------------
  fecha_inicio <- Sys.Date() - 365
  fecha_fin <- Sys.Date()

  URL_base <- "https://connecta.dival.es/apidata/ngsi/v4/history/entities/"
  auth_connecta <- "Bearer 267f0242-4fe3-3ee3-b82e-18b5524916a2"






  #----------------------------------------------------------------------------------------------------------------------------------------
  # LLAMADA Y LIMPIEZA DATOS METEOROLOGIA
  #----------------------------------------------------------------------------------------------------------------------------------------

  # 2.2) DF METEO DE SENTILO

  secuencia_fechas <- seq.Date((Sys.Date() -365),Sys.Date(),30)
  secuencia_fechas <- c(secuencia_fechas, Sys.Date())

  # TEMPERATURA
  DF_TEMP <- data.frame()
  for(i in 1:(length(secuencia_fechas)-1)){
    print(i)
    meteo_spot <- "METEOTOSEC01S01"

    endpoint_get_parking <- paste(URL_base, meteo_spot,"/attrs/temperature?from=",secuencia_fechas[i],"%2000%3A00%3A00&to=", secuencia_fechas[i+1],"%2000%3A00%3A00&limit=",sep = "")
    GET <- httr::GET(url = endpoint_get_parking,
                     add_headers("accept"="application/json","Authorization"=auth_connecta),
                     encode = "json",verbose()
    )

    resultado <- httr::content(GET)
    if(length(resultado) == 0){next}
    # Paso de lista a df
    df_temp <- resultado$values
    df_temp <- do.call(rbind.data.frame, df_temp)

    DF_TEMP <- rbind(DF_TEMP, df_temp)
  }


  # PRECIPITACIÓN
  DF_PREC <- data.frame()
  for(i in 1:(length(secuencia_fechas)-1)){
    print(i)
    meteo_spot <- "METEOTOSEC01S06"

    endpoint_get_parking <- paste(URL_base, meteo_spot,"/attrs/precipitation?from=",secuencia_fechas[i],"%2000%3A00%3A00&to=", secuencia_fechas[i+1],"%2000%3A00%3A00&limit=",sep = "")
    GET <- httr::GET(url = endpoint_get_parking,
                     add_headers("accept"="application/json","Authorization"=auth_connecta),
                     encode = "json",verbose()
    )

    resultado <- httr::content(GET)
    if(length(resultado) == 0){next}
    # Paso de lista a df
    df_prec <- resultado$values
    df_prec <- do.call(rbind.data.frame, df_prec)

    DF_PREC <- rbind(DF_PREC, df_prec)
  }



  # TEMPERATURA
  DF_TEMP <- DF_TEMP[,c(2,3)]
  DF_TEMP <- unique(DF_TEMP)
  DF_TEMP$createdAt <- as_datetime(DF_TEMP$createdAt, tz = 'Europe/Brussels')

  DF_TEMP$createdAt <- as.POSIXct(DF_TEMP$createdAt, format="%Y-%m-%d %H:%M:%S")  # Se evita normalizar la fecha
  DF_TEMP$hora <- format(DF_TEMP$createdAt, format = "%H:%M:%S")
  DF_TEMP$fecha <- format(DF_TEMP$createdAt, format = "%Y-%m-%d")
  DF_TEMP$fecha <- as.Date(DF_TEMP$fecha)
  DF_TEMP$hora <- hms(DF_TEMP$hora)  # Paquete lubridate para compara fechas

  # Categorización de hora
  DF_TEMP$hora_categorizada <- rep(0, nrow(DF_TEMP))
  for(i in 1:nrow(DF_TEMP)){
    hora_cat <- gsub("H.*","",DF_TEMP$hora[i])
    if(grepl("M",hora_cat)){
      hora_cat <- 0
    }else{
      hora_cat <- as.numeric(hora_cat)
    }
    DF_TEMP$hora_categorizada[i] <- hora_cat
  }
  DF_TEMP <- na.omit(DF_TEMP)
  DF_TEMP <- DF_TEMP[,c(1,4,5)]
  DF_TEMP <- unique(DF_TEMP)

  #Promedio temperatura por hora y día
  DF_TEMP <- DF_TEMP %>%
    group_by(fecha, hora_categorizada) %>%
    dplyr::summarize(value = mean(value, na.rm=TRUE))
  DF_TEMP$value <- as.integer(DF_TEMP$value)

  # PRECIPITACIÓN
  DF_PREC <- DF_PREC[,c(2,3)]
  DF_PREC <- unique(DF_PREC)
  DF_PREC$createdAt <- as_datetime(DF_PREC$createdAt, tz = 'Europe/Brussels')

  DF_PREC$createdAt <- as.POSIXct(DF_PREC$createdAt, format="%Y-%m-%d %H:%M:%S")  # Se evita normalizar la fecha
  DF_PREC$hora <- format(DF_PREC$createdAt, format = "%H:%M:%S")
  DF_PREC$fecha <- format(DF_PREC$createdAt, format = "%Y-%m-%d")
  DF_PREC$fecha <- as.Date(DF_PREC$fecha)
  DF_PREC$hora <- hms(DF_PREC$hora)  # Paquete lubridate para compara fechas

  # Categorización de hora
  DF_PREC$hora_categorizada <- rep(0, nrow(DF_PREC))
  for(i in 1:nrow(DF_PREC)){
    hora_cat <- gsub("H.*","",DF_PREC$hora[i])
    if(grepl("M",hora_cat)){
      hora_cat <- 0
    }else{
      hora_cat <- as.numeric(hora_cat)
    }
    DF_PREC$hora_categorizada[i] <- hora_cat
  }
  DF_PREC <- na.omit(DF_PREC)
  DF_PREC <- DF_PREC[,c(1,4,5)]
  DF_PREC <- unique(DF_PREC)
  DF_PREC$value <- as.integer(DF_PREC$value)

  # Ajuste dataframes
  df_meteo <- inner_join(DF_TEMP, DF_PREC, by=c("fecha","hora_categorizada"))
  colnames(df_meteo)[match("value.x",colnames(df_meteo))] <- "temperatura"
  colnames(df_meteo)[match("value.y",colnames(df_meteo))] <- "precipitacion"





  #-----------------------------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------------------------
  # FOR POR CADA UNO DE LOS SENSORES
  #-----------------------------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------------------------

  cuerpo <- '{"username":"kepa@movilidad.es","password":"kepatech"}'
  post <- httr::POST(url = "http://88.99.184.239:30951/api/auth/login",
                     add_headers("Content-Type"="application/json","Accept"="application/json"),
                     body = cuerpo,
                     verify= FALSE,
                     encode = "json",verbose()
  )

  resultado_peticion_token <- httr::content(post)
  auth_thb <- paste("Bearer",resultado_peticion_token$token)



  # GET DISPOSITIVOS PLATAFORMA
  url_thb <- "http://88.99.184.239:30951/api/tenant/devices?pageSize=10000&page=0"
  peticion <- GET(url_thb, add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb))
  df <- jsonlite::fromJSON(rawToChar(peticion$content))
  df <- as.data.frame(df)
  df_dispositivos_prediccion <- df[df$data.type == "Predicción aparcamiento",] # Filtrado por GPS

  ids_prediccion <- df_dispositivos_prediccion$data.id$id


  for(id_pred_i in 1:length(ids_prediccion)){
    print("---------------------------------------------------------------------------------")
    print(id_pred_i)

    #-----------------------------------------------------------------------
    # LLAMADA GET DATOS HISTÓRICOS
    #-----------------------------------------------------------------------

    parking_spot <- paste(df_dispositivos_prediccion$data.name[id_pred_i],"S01",sep = "")

    endpoint_get_parking <- paste(URL_base, parking_spot,"/attrs/status?from=",fecha_inicio,"%2000%3A00%3A00&to=", fecha_fin,"%2000%3A00%3A00&limit=",sep = "")
    GET <- httr::GET(url = endpoint_get_parking,
                     add_headers("accept"="application/json","Authorization"=auth_connecta),
                     encode = "json",verbose()
    )

    if(GET$status_code != 200){
      print("ERROR EN LA LLAMADA")
      next
    }

    resultado <- httr::content(GET)

    if(length(resultado) == 0){
      print(" SIN DATOS DEL SENSOR")
      next
    }

    # Paso de lista a df
    df <- resultado$values
    df <- do.call(rbind.data.frame, df)
    colnames(df) <- c("ts_muestra","valor","ts_creacion")
    #Paso a hora local
    df$ts_muestra <- as_datetime(df$ts_muestra, tz = 'Europe/Brussels')
    df$ts_creacion <- as_datetime(df$ts_creacion, tz = 'Europe/Brussels')

    df_parking <- df



    #----------------------------------------------------------------------------------------------------------------------------------------
    # LIMPIEZA DATOS
    #----------------------------------------------------------------------------------------------------------------------------------------
    # 1) DF PARKING
    df_prediccion <- df_parking
    df_prediccion$valor[df_prediccion$valor != "LIBRE"] <- 1
    df_prediccion$valor[df_prediccion$valor == "LIBRE"] <- 0




    #----------------------------------------------------------------------------------------------------------------------------------------
    # NORMALIZACIÓN
    #----------------------------------------------------------------------------------------------------------------------------------------
    normaizado_func <- function(x){(x-min(x))/(max(x)-min(x))}
    col_numericas <- sapply(df_prediccion, is.numeric)
    col_numericas <- grep(TRUE,col_numericas)
    datos_normalizados <- df_prediccion
    for (i in col_numericas){
      datos_normalizados[,i] <- normaizado_func(datos_normalizados[,i])
    }

    datos_normalizados$valor <- as.numeric(datos_normalizados$valor)



    #----------------------------------------------------------------------------------------------------------------------------------------
    # INGENIERÍA DE CARACTERÍSTICAS
    #----------------------------------------------------------------------------------------------------------------------------------------

    datos_normalizados <- datos_normalizados[,c(1,2)]

    datos_normalizados$ts_muestra <- as.POSIXct(datos_normalizados$ts_muestra, format="%Y-%m-%d %H:%M:%S")  # Se evita normalizar la fecha
    datos_normalizados$hora <- format(datos_normalizados$ts_muestra, format = "%H:%M")
    datos_normalizados$fecha <- format(datos_normalizados$ts_muestra, format = "%Y-%m-%d")
    datos_normalizados$fecha <- as.Date(datos_normalizados$fecha)
    #datos_normalizados$hora <- hms(datos_normalizados$hora)  # Paquete lubridate para compara fechas

    # Hora categorizada
    datos_normalizados$hora_categorizada <- hour(datos_normalizados$ts_muestra)
    datos_normalizados$minutos <- minute(datos_normalizados$ts_muestra)
    datos_normalizados <- na.omit(datos_normalizados)

    for(i in 1:nrow(datos_normalizados)){
      if(datos_normalizados$minutos[i] >= 0 && datos_normalizados$minutos[i] <=15){
        datos_normalizados$minutos[i] <- 1
      }else if(datos_normalizados$minutos[i] > 15 && datos_normalizados$minutos[i] <=30){
        datos_normalizados$minutos[i] <- 1
      }else if(datos_normalizados$minutos[i] > 31 && datos_normalizados$minutos[i] <=45){
        datos_normalizados$minutos[i] <- 2
      }else if(datos_normalizados$minutos[i] > 45 && datos_normalizados$minutos[i] <=60){
        datos_normalizados$minutos[i] <- 2
      }
    }


    # 3.3 FIMES DE SEMANA & FESTIVOS (1) VS LABORABLES (0)
    # Extracción calendario laboral Logroño
    #peticion_dias_festivos <- GET("https://holydayapi.herokuapp.com/holidays/city_code/26089/year/2021", add_headers("Content-Type"="application/json","Accept"="application/json"))
    #df_festivos <- jsonlite::fromJSON(rawToChar(peticion_dias_festivos$content))
    #df_festivos$day <- as.Date(df_festivos$day,"%d/%m/%Y")
    #df_festivos$day <- as.Date(df_festivos$day,"%Y-%m-%d")

    # Desagregación fines de semana
    datos_normalizados$fin_semana_festivo <- weekdays(datos_normalizados$fecha)
    datos_normalizados$fin_semana_festivo <- ifelse(datos_normalizados$fin_semana_festivo == "sábado" | datos_normalizados$fin_semana_festivo == "domingo",1,0)
    #datos_normalizados$fin_semana_festivo[which(datos_normalizados$fecha %in% df_festivos$day)] <- 1  # A los festivos también les asigno un 1

    datos_normalizados <- datos_normalizados[order(datos_normalizados$ts_muestra),]  # Orden por fecha

    #
    # INCORPORACIÓN DE VARIABLES METEO
    #
    datos_normalizados <- inner_join(datos_normalizados, df_meteo, by=c("fecha","hora_categorizada"))


    # Normalización variables meteo
    datos_normalizados$temperatura <- as.numeric(datos_normalizados$temperatura)
    datos_normalizados$precipitacion <- as.numeric(datos_normalizados$precipitacion)
    #datos_normalizados$temperatura <- normaizado_func(datos_normalizados$temperatura )
    datos_normalizados$temperatura[datos_normalizados$temperatura > 0 & datos_normalizados$temperatura <=5] <- 0
    datos_normalizados$temperatura[datos_normalizados$temperatura > 5 & datos_normalizados$temperatura <=10] <- 1
    datos_normalizados$temperatura[datos_normalizados$temperatura > 10 & datos_normalizados$temperatura <=15] <- 2
    datos_normalizados$temperatura[datos_normalizados$temperatura > 15 & datos_normalizados$temperatura <=20] <- 3
    datos_normalizados$temperatura[datos_normalizados$temperatura > 20 & datos_normalizados$temperatura <=25] <- 4
    datos_normalizados$temperatura[datos_normalizados$temperatura > 25 & datos_normalizados$temperatura <=30] <- 5
    datos_normalizados$temperatura[datos_normalizados$temperatura > 30] <- 6

    # Generación de días de la semana  s.POSIXlt(df$date)$wday
    datos_normalizados$dia_semana <- as.POSIXlt(datos_normalizados$fecha)$wday
    datos_normalizados$dia_mes <- as.POSIXlt(datos_normalizados$fecha)$mday
    datos_normalizados$mes <- month(datos_normalizados$fecha)

    datos_normalizados <- datos_normalizados[,-c(1,4)]
    datos_normalizados <- unique(datos_normalizados)

    datos_normalizados_origen <- datos_normalizados



    #========================================================================================================================
    #========================================================================================================================

    # MODELOS DE PREDICCIÓN: KNN

    #========================================================================================================================
    #========================================================================================================================

    datos_normalizados$valor <- as.numeric(datos_normalizados$valor)
    datos_normalizados <- datos_normalizados[,-c(2)]  # Solo valores numéricos
    datos_normalizados <- na.omit(datos_normalizados)
    datos_normalizados <- unique(datos_normalizados)

    for(k in 1:ncol(datos_normalizados)){
      datos_normalizados[,k] <- as.factor(datos_normalizados[,k])
    }

    set.seed(123)

    default_idx = sample(nrow(datos_normalizados), 0.7*nrow(datos_normalizados))  # Separación 70-30
    default_trn = datos_normalizados[default_idx, ]
    default_tst = datos_normalizados[-default_idx, ]

    # Datos entrenamiento
    pos_valor <- grep("valor", colnames(default_trn))
    X_default_trn = default_trn[, -pos_valor]
    #X_default_trn = default_trn
    y_default_trn = default_trn$valor

    # Datos test
    pos_presencia <- grep("valor", colnames(default_tst))
    X_default_tst = default_tst[, -pos_valor]
    #X_default_tst = default_tst
    y_default_tst = default_tst$valor


    #========================================================================
    # Selección de k óptimo
    set.seed(123)
    k_to_try = 1:100
    err_k = rep(x = 0, times = length(k_to_try))

    calc_class_err = function(actual, predicted) {
      mean(actual != predicted)
    }

    for (i in seq_along(k_to_try)) {
      pred = knn(train = X_default_trn,
                 test  = X_default_tst,
                 cl    = y_default_trn,
                 k     = k_to_try[i])
      err_k[i] = calc_class_err(y_default_tst, pred)
    }

    min(err_k[err_k != 0])
    which(err_k == min(err_k[err_k != 0]))
    k_optimo <- max(which(err_k ==min(err_k[err_k != 0])))

    if(k_optimo == -Inf){
      print("K ÓPTIMO == INFINITO")
      next
    }


    table(y_default_tst)
    #============================

    # Predicción
    head(class::knn(train = X_default_trn, test = X_default_tst, cl = y_default_trn, k= k_optimo))
    prediccion <- class::knn(train = X_default_trn, test = X_default_tst, cl = y_default_trn, k= k_optimo)

    # Evaluación modelo
    matriz_confusion <- confusionMatrix(table(prediccion ,y_default_tst))
    print(confusionMatrix(table(prediccion ,y_default_tst)))
    precision_matriz <- matriz_confusion[["overall"]][["Accuracy"]]
    resultado_prediccion <- data.frame(prediccion, default_tst$valor)
    tabla_matriz_confusión <- matriz_confusion[["table"]]

    if(matriz_confusion[["overall"]][["Accuracy"]] < 0.7){
      print("PRECIÓN DE LA PREDICCIÓN < 70%")
      next
    }


    #========================================================================================================================
    #========================================================================================================================

    # PREDICCIÓN DE SIGUINETE: 1 hora; 2 horas y 5 horas (tramos de 30 minutos)

    #========================================================================================================================
    #========================================================================================================================

    # Generación de las sigientes 5 horas
    datos_normalizados_prediccion <- datos_normalizados
    datos_normalizados_prediccion <- datos_normalizados_prediccion[,-1]

    temperatura <- rep(datos_normalizados_prediccion$temperatura[nrow(datos_normalizados_prediccion)],10)
    precipitacion <- rep(datos_normalizados_prediccion$precipitacion[nrow(datos_normalizados_prediccion)],10)
    dia_semana <- rep(datos_normalizados_prediccion$dia_semana[nrow(datos_normalizados_prediccion)],10)
    dia_mes <- rep(datos_normalizados_prediccion$dia_mes[nrow(datos_normalizados_prediccion)],10)
    fin_semana_festivo <- rep(0,10)
    hora_categorizada <- rep(0,10)
    minutos <- rep(0,10)
    mes <- rep(0,10)
    for(i in 1:length(hora_categorizada)){
      print(i)
      if(i == 1){
        if(datos_normalizados_prediccion$minutos[i] == 2){
          minutos[i] <- 1
          if(datos_normalizados_prediccion$hora_categorizada[nrow(datos_normalizados_prediccion)] == 23){
            hora_categorizada[i] <- 0
            if(datos_normalizados_prediccion$fin_semana_festivo[nrow(datos_normalizados_prediccion)] == 1){
              fin_semana_festivo[i] <- as.factor(0)
            }else{
              fin_semana_festivo[i] <- datos_normalizados_prediccion$fin_semana_festivo[nrow(datos_normalizados_prediccion)]
            }
            if(dia_semana[i] == 6){
              dia_semana[i] <- 0
            }else{
              dia_semana[i] <- as.factor(as.numeric(as.character(dia_semana[i]))+1)
            }
          }else{
            hora_categorizada[i] <- as.numeric(as.character(datos_normalizados_prediccion$hora_categorizada[nrow(datos_normalizados_prediccion)])) + 1
            fin_semana_festivo[i] <- datos_normalizados_prediccion$fin_semana_festivo[nrow(datos_normalizados_prediccion)]
          }
        }else{
          hora_categorizada[i] <- datos_normalizados_prediccion$hora_categorizada
          minutos[i] <- 2
          fin_semana_festivo[i] <- datos_normalizados_prediccion$fin_semana_festivo
          dia_semana[i] <- datos_normalizados_prediccion$dia_semana
        }
      }else{
        if(minutos[i-1] == 2){
          minutos[i] <- 1
          if(hora_categorizada[i-1] == 23){
            hora_categorizada[i] <- 0
            if(datos_normalizados_prediccion$fin_semana_festivo[nrow(datos_normalizados_prediccion)] == 1){
              fin_semana_festivo[i] <- as.factor(0)
            }else{
              fin_semana_festivo[i] <- datos_normalizados_prediccion$fin_semana_festivo[nrow(datos_normalizados_prediccion)]
            }
            if(dia_semana[i] == 6){
              dia_semana[i] <- 0
            }else{
              dia_semana[i] <- as.factor(as.numeric(as.character(dia_semana[i]))+ 1)
            }
          }else{
            hora_categorizada[i] <- hora_categorizada[i-1] + 1
            fin_semana_festivo[i] <- datos_normalizados_prediccion$fin_semana_festivo[nrow(datos_normalizados_prediccion)]
          }
        }else{
          minutos[i] <- 2
          hora_categorizada[i] <- hora_categorizada[i-1]
          fin_semana_festivo[i] <- fin_semana_festivo[i-1]
          dia_semana[i] <- dia_semana[i-1]
        }
      }

      if(days_in_month(Sys.Date()) == dia_mes[i]){
        dia_mes[i] <- 1
        if(mes[i] == 12){
          mes[i] <- 1
        }else{
          if(i == 1){
            mes[i] <- mes[i] + 1
          }else{
            mes[i] <- mes[i - 1] +1
          }
        }
      }
    }

    hora_categorizada <- as.factor(hora_categorizada)
    df_2 <- data.frame(hora_categorizada, minutos, fin_semana_festivo, temperatura, precipitacion, dia_semana, dia_mes, mes)

    for(i in 1:ncol(df_2)){
      df_2[,i] <- as.factor(df_2[,i])
    }


    #------------------ PREDICCIÓN -----------------------
    DF_A_PREDECIR <- df_2

    # Predicción
    head(class::knn(train = X_default_trn, test = DF_A_PREDECIR, cl = y_default_trn, k= k_optimo))
    prediccion <- class::knn(train = X_default_trn, test = DF_A_PREDECIR, cl = y_default_trn, k= k_optimo)

    # Predicción a texto
    prediccion_texto <- ifelse(prediccion == 1, "OCUPADO", "LIBRE")


    json <- paste('{"Estado próxima media hora (30\')":"',prediccion_texto[1],
                  '", "Estado próxima hora (1h)":"',prediccion_texto[2],
                  '", "Estado próxima hora y media (1h 30\')":"',prediccion_texto[3],
                  '", "Estado próxima 2ª hora (2h)":"',prediccion_texto[4],'"}',sep = "")


    #========================================================================================================================
    #========================================================================================================================

    # ENVÍO A PLATAFORMA

    #========================================================================================================================
    #========================================================================================================================

    id_disp <- ids_prediccion[id_pred_i]

    url <- paste("http://88.99.184.239:30951/api/plugins/telemetry/DEVICE/",id_disp,"/timeseries/ANY?scope=ANY",sep = "")
    post <- httr::POST(url = url,
                       add_headers("Content-Type"="application/json","Accept"="application/json","X-Authorization"=auth_thb),
                       body = json,
                       verify= FALSE,
                       encode = "json",verbose()
    )

  }


  return(1)


}
