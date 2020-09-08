# @file ApplyPlp.R
#
# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of PatientLevelPrediction
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Apply train model on new data
#' Apply a Patient Level Prediction model on Patient Level Prediction Data and get the predicted risk
#' in [0,1] for each person in the population. If the user inputs a population with an outcomeCount
#' column then the function also returns the evaluation of the prediction (AUC, brier score,
#' calibration)
#'
#' @param population       The population of people who you want to predict the risk for
#' @param plpData          The plpData for the population
#' @param plpModel         The trained PatientLevelPrediction model
#' @param calculatePerformance  Whether to also calculate the performance metrics [default TRUE]
#' @param databaseOutput   Whether to save the details into the prediction database
#' @param silent           Whether to turn off progress reporting
#'
#' @examples
#' \dontrun{
#' # load the model and data
#' plpData <- loadPlpData("C:/plpdata")
#' plpModel <- loadPlpModel("C:/plpmodel")
#'
#' # use the same population settings as the model:
#' populationSettings <- plpModel$populationSettings
#' populationSettings$plpData <- plpData
#' population <- do.call(createStudyPopulation, populationSettings)
#'
#' # get the prediction:
#' prediction <- applyModel(population, plpData, plpModel)$prediction
#' }
#' @export
applyModel <- function(population,
                       plpData,
                       plpModel,
                       calculatePerformance=T,
                       databaseOutput = NULL,
                       silent = F) {
  
  # check logger
  if(length(ParallelLogger::getLoggers())==0){
    logger <- ParallelLogger::createLogger(name = "SIMPLE",
                                           threshold = "INFO",
                                           appenders = list(ParallelLogger::createConsoleAppender(layout = 'layoutTimestamp')))
    ParallelLogger::registerLogger(logger)
  }
  
  # check input:
  if (is.null(population))
    stop("NULL population")
  if (class(plpData) != "plpData")
    stop("Incorrect plpData class")
  if (class(plpModel) != "plpModel")
    stop("Incorrect plpModel class")
  
  # log the trained model details TODO
  
  # get prediction counts:
  peopleCount <- nrow(population)
  
  start.pred <- Sys.time()
  if (!silent)
    ParallelLogger::logInfo(paste("Starting Prediction ", Sys.time(), "for ", peopleCount, " people"))
  
  prediction <- plpModel$predict(plpData = plpData, population = population)
  
  
  if (!silent)
    ParallelLogger::logInfo(paste("Prediction completed at ", Sys.time(), " taking ", start.pred - Sys.time()))
  
  
  if (!"outcomeCount" %in% colnames(prediction))
    return(list(prediction = prediction))
  
  if(!calculatePerformance || nrow(prediction) == 1)
    return(list(prediction = prediction))
  
  if (!silent)
    ParallelLogger::logInfo(paste("Starting evaulation at ", Sys.time()))
  
  performance <- PatientLevelPrediction::evaluatePlp(prediction, plpData)
  
  # reformatting the performance 
  analysisId <-   '000000'
  if(!is.null(plpModel$analysisId)){
    analysisId <-   plpModel$analysisId
  }
  
  nr1 <- length(unlist(performance$evaluationStatistics[-1]))
  performance$evaluationStatistics <- cbind(analysisId= rep(analysisId,nr1),
                                            Eval=rep('validation', nr1),
                                            Metric = names(unlist(performance$evaluationStatistics[-1])),
                                            Value = unlist(performance$evaluationStatistics[-1])
  )
  nr1 <- nrow(performance$thresholdSummary)
  performance$thresholdSummary <- cbind(analysisId=rep(analysisId,nr1),
                                        Eval=rep('validation', nr1),
                                        performance$thresholdSummary)
  nr1 <- nrow(performance$demographicSummary)
  if(!is.null(performance$demographicSummary)){
    performance$demographicSummary <- cbind(analysisId=rep(analysisId,nr1),
                                            Eval=rep('validation', nr1),
                                            performance$demographicSummary)
  }
  nr1 <- nrow(performance$calibrationSummary)
  performance$calibrationSummary <- cbind(analysisId=rep(analysisId,nr1),
                                          Eval=rep('validation', nr1),
                                          performance$calibrationSummary)
  nr1 <- nrow(performance$predictionDistribution)
  performance$predictionDistribution <- cbind(analysisId=rep(analysisId,nr1),
                                              Eval=rep('validation', nr1),
                                              performance$predictionDistribution)
  
  
  if (!silent)
    ParallelLogger::logInfo(paste("Evaluation completed at ", Sys.time(), " taking ", start.pred - Sys.time()))
  
  if (!silent)
    ParallelLogger::logInfo(paste("Starting covariate summary at ", Sys.time()))
  start.pred  <- Sys.time()
  covSum <- covariateSummary(plpData, population)
  if(exists("plpModel")){
    if(!is.null(plpModel$varImp)){
      covSum <- merge(plpModel$varImp[,colnames(plpModel$varImp)!='covariateName'], covSum, by='covariateId', all=T)
    }
  }
  
  if (!silent)
    ParallelLogger::logInfo(paste("Covariate summary completed at ", Sys.time(), " taking ", start.pred - Sys.time()))
  
  executionSummary <- list(PackageVersion = list(rVersion= R.Version()$version.string,
                                                 packageVersion = utils::packageVersion("PatientLevelPrediction")),
                           PlatformDetails= list(platform= R.Version()$platform,
                                                 cores= Sys.getenv('NUMBER_OF_PROCESSORS'),
                                                 RAM=utils::memory.size()), #  test for non-windows needed
                           # Sys.info()
                           TotalExecutionElapsedTime = NULL,
                           ExecutionDateTime = Sys.Date())
  
  result <- list(prediction = prediction, 
                 performanceEvaluation = performance,
                 inputSetting = list(outcomeId=attr(population, "metaData")$outcomeId,
                                     cohortId= plpData$metaData$call$cohortId,
                                     database = plpData$metaData$call$cdmDatabaseSchema),
                 executionSummary = executionSummary,
                 model = list(model='applying plp model',
                              modelAnalysisId = plpModel$analysisId,
                              modelSettings = plpModel$modelSettings),
                 analysisRef=list(analysisId=NULL,
                                  analysisName=NULL,
                                  analysisSettings= NULL),
                 covariateSummary=covSum)
  return(result)
}


#' Extract new plpData using plpModel settings
#' use metadata in plpModel to extract similar data and population for new databases:
#'
#' @param plpModel         The trained PatientLevelPrediction model or object returned by runPlp()
#' @param createCohorts          Create the tables for the target and outcome - requires sql in the plpModel object
#' @param newConnectionDetails      The connectionDetails for the new database
#' @param newCdmDatabaseSchema      The database schema for the new CDM database 
#' @param newCohortDatabaseSchema   The database schema where the cohort table is stored
#' @param newCohortTable            The table name of the cohort table
#' @param newCohortId               The cohort_definition_id for the cohort of at risk people
#' @param newOutcomeDatabaseSchema  The database schema where the outcome table is stored
#' @param newOutcomeTable           The table name of the outcome table
#' @param newOutcomeId              The cohort_definition_id for the outcome  
#' @param newOracleTempSchema       The temp coracle schema
#' @param sample                    The number of people to sample (default is NULL meaning use all data)
#' @param createPopulation          Whether to create the study population as well
#'
#' @examples
#' \dontrun{
#' # set the connection
#' connectionDetails <- DatabaseConnector::createConnectionDetails()
#'    
#' # load the model and data
#' plpModel <- loadPlpModel("C:/plpmodel")
#'
#' # extract the new data in the 'newData.dbo' schema using the model settings 
#' newDataList <- similarPlpData(plpModel=plpModel, 
#'                               newConnectionDetails = connectionDetails,
#'                               newCdmDatabaseSchema = 'newData.dbo',
#'                               newCohortDatabaseSchema = 'newData.dbo',   
#'                               newCohortTable = 'cohort', 
#'                               newCohortId = 1, 
#'                               newOutcomeDatabaseSchema = 'newData.dbo', 
#'                               newOutcomeTable = 'outcome',     
#'                               newOutcomeId = 2)    
#'                
#' # get the prediction:
#' prediction <- applyModel(newDataList$population, newDataList$plpData, plpModel)$prediction
#' }
#' @export
similarPlpData <- function(plpModel=NULL,
                           createCohorts = T,
                           newConnectionDetails,
                           newCdmDatabaseSchema = NULL,
                           newCohortDatabaseSchema = NULL,
                           newCohortTable = NULL,
                           newCohortId = NULL,
                           newOutcomeDatabaseSchema = NULL,
                           newOutcomeTable = NULL,
                           newOutcomeId = NULL,
                           newOracleTempSchema = newCdmDatabaseSchema,
                           sample=NULL, 
                           createPopulation= T) {
  
  # check logger
  if(length(ParallelLogger::getLoggers())==0){
    logger <- ParallelLogger::createLogger(name = "SIMPLE",
                                           threshold = "INFO",
                                           appenders = list(ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)))
    ParallelLogger::registerLogger(logger)
  }
  
  if(is.null(plpModel))
    return(NULL)
  if(class(plpModel)!='plpModel' && class(plpModel)!='runPlp' )
    return(NULL)
  if(class(plpModel)=='runPlp')
    plpModel <- plpModel$model 
  
  if(missing(newConnectionDetails)){
    stop('connection details not entered')
  } else {
    connection <- DatabaseConnector::connect(newConnectionDetails)
  }
  
  if(createCohorts){
    if(is.null(plpModel$metaData$cohortCreate$targetCohort$sql))
      stop('No target cohort code')
    if(is.null(plpModel$metaData$cohortCreate$outcomeCohorts[[1]]$sql))
      stop('No outcome cohort code')
    
    
    exists <- toupper(newCohortTable)%in%DatabaseConnector::getTableNames(connection , newCohortDatabaseSchema)
    if(!exists){
      ParallelLogger::logTrace('Creating temp cohort table')
      sql <- "create table @target_cohort_schema.@target_cohort_table(cohort_definition_id bigint, subject_id bigint, cohort_start_date datetime, cohort_end_date datetime)"
      sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
      sql <- SqlRender::renderSql(sql,
                                  target_cohort_schema = newCohortDatabaseSchema,
                                  target_cohort_table= newCohortTable)$sql
      tryCatch(DatabaseConnector::executeSql(connection,sql),
               error = stop, finally = ParallelLogger::logTrace('Cohort table created'))
    }
    
    exists <- toupper(newOutcomeTable)%in%DatabaseConnector::getTableNames(connection , newOutcomeDatabaseSchema)
    if(!exists){
      sql <- "create table @target_cohort_schema.@target_cohort_table(cohort_definition_id bigint, subject_id bigint, cohort_start_date datetime, cohort_end_date datetime)"
      sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
      sql <- SqlRender::renderSql(sql,
                                  target_cohort_schema = newOutcomeDatabaseSchema,
                                  target_cohort_table= newOutcomeTable)$sql
      tryCatch(DatabaseConnector::executeSql(connection,sql),
               error = stop, finally = ParallelLogger::logTrace('outcome table created'))
      
    }
    
    ParallelLogger::logTrace('Populating cohort tables')
    targetSql <- plpModel$metaData$cohortCreate$targetCohort$sql
    targetSql <- SqlRender::renderSql(targetSql, 
                                      cdm_database_schema=ifelse(is.null(newCdmDatabaseSchema),plpModel$metaData$call$cdmDatabaseSchema,newCdmDatabaseSchema),
                                      target_database_schema= ifelse(is.null(newCohortDatabaseSchema),plpModel$metaData$call$cdmDatabaseSchema,newCohortDatabaseSchema),
                                      target_cohort_table = ifelse(is.null(newCohortTable),plpModel$metaData$call$newCohortTable,newCohortTable),
                                      target_cohort_id = ifelse(is.null(newCohortId),plpModel$metaData$call$cohortId, newCohortId) )$sql
    
    targetSql <- SqlRender::translateSql(targetSql, 
                                         targetDialect = ifelse(is.null(newConnectionDetails$dbms), 'pdw',newConnectionDetails$dbms)  )$sql
    DatabaseConnector::executeSql(connection, targetSql)
    
    for(outcomesql in plpModel$metaData$cohortCreate$outcomeCohorts){
      outcomeSql <- outcomesql$sql
      outcomeSql <- SqlRender::renderSql(outcomeSql, 
                                         cdm_database_schema=ifelse(is.null(newCdmDatabaseSchema),plpModel$metaData$call$cdmDatabaseSchema,newCdmDatabaseSchema),
                                         target_database_schema= ifelse(is.null(newOutcomeDatabaseSchema),plpModel$metaData$call$cdmDatabaseSchema,newOutcomeDatabaseSchema),
                                         target_cohort_table = ifelse(is.null(newOutcomeTable),plpModel$metaData$call$newOutcomeTable,newOutcomeTable),
                                         target_cohort_id = ifelse(is.null(newOutcomeId),plpModel$metaData$call$outcomeId, newOutcomeId))$sql
      outcomeSql <- SqlRender::translateSql(outcomeSql, 
                                            targetDialect = ifelse(is.null(newConnectionDetails$dbms), 'pdw',newConnectionDetails$dbms))$sql
      DatabaseConnector::executeSql(connection, outcomeSql)
      
    }
    
    
  }
  
  ParallelLogger::logTrace('Loading model data extraction settings')
  dataOptions <- as.list(plpModel$metaData$call)
  dataOptions[[1]] <- NULL
  dataOptions$sampleSize <- sample
  
  dataOptions$covariateSettings$includedCovariateIds <-  plpModel$varImp$covariateId[plpModel$varImp$covariateValue!=0]
  
  ParallelLogger::logTrace('Adding new settings if set...')
  if(is.null(newCdmDatabaseSchema))
    return(NULL)
  dataOptions$cdmDatabaseSchema <- newCdmDatabaseSchema
  
  if(!is.null(newConnectionDetails))
    dataOptions$connectionDetails <- newConnectionDetails # check name
  
  if(!is.null(newCohortId))
    dataOptions$cohortId <- newCohortId
  if(!is.null(newOutcomeId))
    dataOptions$outcomeIds <- newOutcomeId
  
  if(!is.null(newCohortDatabaseSchema))
    dataOptions$cohortDatabaseSchema <- newCohortDatabaseSchema  # correct names?
  if(!is.null(newCohortTable))
    dataOptions$cohortTable <- newCohortTable
  
  if(!is.null(newOutcomeDatabaseSchema))
    dataOptions$outcomeDatabaseSchema <- newOutcomeDatabaseSchema # correct names?
  if(!is.null(newOutcomeTable))
    dataOptions$outcomeTable <- newOutcomeTable
  if(!is.null(newOracleTempSchema))
    dataOptions$oracleTempSchema <- newOracleTempSchema # check name
  
  
  dataOptions$baseUrl <- NULL
  
  plpData <- do.call(getPlpData, dataOptions)
  
  if(!createPopulation) return(plpData)
  
  # get the popualtion
  ParallelLogger::logTrace('Loading model population settings')
  popOptions <- plpModel$populationSettings
  popOptions$cohortId <- dataOptions$cohortId
  popOptions$outcomeId <- dataOptions$outcomeIds
  popOptions$plpData <- plpData
  population <- do.call(CoDImputationOnlyDeathPop::createStudyPopulation, popOptions)
  
  
  # return the popualtion and plpData for the new database
  ParallelLogger::logTrace('Returning population and plpData for new data using model settings')
  return(list(population=population,
              plpData=plpData))
}


# this function calcualtes:
# CovariateCount	CovariateCountWithOutcome	
# CovariateCountWithNoOutcome	CovariateMeanWithOutcome	
# CovariateMeanWithNoOutcome	CovariateStDevWithOutcome	
# CovariateStDevWithNoOutcome	CovariateStandardizedMeanDifference
covariateSummary <- function(plpData, population = NULL, model = NULL){
  #===========================
  # all 
  #===========================
  if(!is.null(model$varImp)){
    variableImportance <- tibble::as_tibble(model$varImp[,colnames(model$varImp)!='covariateName'])
  } else{
    variableImportance <- tibble::tibble(covariateId = 1,
                                         covariateValue = 0)
  }
  plpData$covariateData$variableImportance <- variableImportance
  on.exit(plpData$covariateData$variableImportance <- NULL, add = TRUE)
  
  
  # restrict to pop
  if(!is.null(population)){
    covariates <- plpData$covariateData$covariates %>% 
      dplyr::filter(rowId %in% local(population$rowId)) 
  } else{
    covariates <- plpData$covariateData$covariates
  }
  
  
  # find total pop values:
  N <- nrow(population)
  means <- covariates %>%
    dplyr::group_by(covariateId) %>%
    dplyr::summarise(CovariateMean = 1.0*sum(covariateValue,na.rm = TRUE)/N,
                     CovariateCount = dplyr::n()) %>%
    dplyr::select(covariateId,CovariateMean,CovariateCount)
  
  allResult <- covariates %>% 
    dplyr::inner_join(means, by= 'covariateId') %>%
    dplyr::group_by(covariateId, CovariateMean, CovariateCount) %>%
    dplyr::summarise(meanDiff = sum((1.0*CovariateMean - covariateValue)^2, na.rm = TRUE) ) %>%
    dplyr::mutate(CovariateStDev = sqrt( ( meanDiff + CovariateMean^2*(N - CovariateCount )  )/(N-1)  )) %>%
    dplyr::select(covariateId,CovariateCount,CovariateMean,CovariateStDev) 
  
  #allResult <- tibble::as_tibble(as.data.frame(plpData$covariateData$covariateRef %>%
  #  dplyr::select('covariateId', 'covariateName') %>%
  #  dplyr::inner_join(allResult) %>%
  #  dplyr::left_join(plpData$covariateData$variableImportance, by = 'covariateId')))
  allResult <- plpData$covariateData$covariateRef %>%
    dplyr::select('covariateId', 'covariateName') %>%
    dplyr::inner_join(allResult) %>%
    dplyr::left_join(plpData$covariateData$variableImportance, by = 'covariateId') %>% 
    dplyr::collect()
  
  # add with and without outcome
  if('outcomeCount' %in% colnames(population)){
    plpData$covariateData$population <- tibble::as_tibble(population) %>% 
      dplyr::group_by(outcomeCount) %>%
      dplyr::summarise(Ns= dplyr::n()) %>% 
      dplyr::inner_join(tibble::as_tibble(population), by = 'outcomeCount') %>%
      dplyr::select('rowId', 'outcomeCount', 'Ns')
    on.exit(plpData$covariateData$population  <- NULL, add = TRUE)
    
    means <- covariates %>% 
      dplyr::inner_join(plpData$covariateData$population, by = 'rowId') %>%
      dplyr::group_by(covariateId, outcomeCount, Ns) %>%
      dplyr::summarise(CovariateMean = 1.0*sum(covariateValue,na.rm = TRUE)/Ns) %>%
      dplyr::select(covariateId,outcomeCount,CovariateMean)
    #on.exit(plpData$covariateData$means  <- NULL, add = TRUE)
    
    oStratResult <- covariates %>% 
      dplyr::inner_join(plpData$covariateData$population, by = 'rowId') %>%
      dplyr::inner_join(means, by= c('covariateId','outcomeCount') ) %>%
      dplyr::group_by(covariateId, outcomeCount, CovariateMean, Ns) %>%
      dplyr::summarise(CovariateCount = dplyr::n(),
                       meanDiff = sum((1.0*CovariateMean - covariateValue)^2, na.rm = TRUE) ) %>%
      dplyr::mutate(CovariateStDev = sqrt( ( meanDiff + CovariateMean^2*(Ns - CovariateCount )  )/(Ns-1)  )) %>%
      dplyr::select(covariateId,outcomeCount,CovariateCount,CovariateMean,CovariateStDev) %>%
      dplyr::collect()
    
    ##oStratResult <- tibble::as_tibble(as.data.frame(oStratResult))
    # make the columns by outcome...
    out <- oStratResult %>% 
      data.frame() %>% 
      dplyr::filter( outcomeCount == 1 ) %>%
      dplyr::select(covariateId, CovariateCount, CovariateMean,CovariateStDev)
    colnames(out)[-1] <- paste0(colnames(out)[-1], 'WithOutcome')
    noout <- oStratResult %>% 
      data.frame() %>% 
      dplyr::filter( outcomeCount == 0) %>%
      dplyr::select(covariateId, CovariateCount, CovariateMean,CovariateStDev)
    colnames(noout)[-1] <- paste0(colnames(noout)[-1], 'WithNoOutcome')
    
    allResult <- allResult %>% 
      dplyr::full_join(noout, by ='covariateId') %>%
      dplyr::full_join(out, by ='covariateId') %>%
      dplyr::mutate(StandardizedMeanDiff = (CovariateMeanWithOutcome-CovariateMeanWithNoOutcome)/sqrt(CovariateStDevWithOutcome^2+CovariateStDevWithNoOutcome^2))
    
  }
  
  # find summary by outcome and index:
  if('indexes' %in% colnames(population)){
    totals <- tibble::as_tibble(population) %>% 
      dplyr::mutate(index = indexes<0) %>% 
      dplyr::group_by(outcomeCount, index) %>%
      dplyr::summarise(Ns= dplyr::n())
    
    plpData$covariateData$population <- tibble::as_tibble(population) %>% 
      dplyr::mutate(index = indexes<0) %>% 
      dplyr::select(rowId, outcomeCount, index) %>% 
      dplyr::inner_join(totals, by = c('outcomeCount','index'))
    on.exit(plpData$covariateData$population <- NULL, add = TRUE)
    
    result <- covariates %>% 
      dplyr::inner_join(plpData$covariateData$population, by= 'rowId') %>%
      dplyr::group_by(covariateId, outcomeCount, index, Ns) %>%
      dplyr::summarise(CovariateCount = dplyr::n(),
                       CovariateMean = 1.0*sum(covariateValue,na.rm = TRUE)/Ns
                       #meanDiff = (1.0*sum(covariateValue,na.rm = TRUE)/Ns - covariateValue)^2
      ) %>%
      #dplyr::mutate(CovariateStDev = sqrt( ( meanDiff + CovariateMean^2*(Ns - CovariateCount )  )/(Ns-1)  )) %>%
      dplyr::select(covariateId,outcomeCount,index, CovariateCount,CovariateMean) %>%
      dplyr::collect()
    
    ##result <- tibble::as_tibble(as.data.frame(result))
    
    # selecting the test/train/class 
    nonOutTest <- result %>% data.frame() %>%
      dplyr::filter( outcomeCount == 0 & index == T ) %>%
      dplyr::select(covariateId, CovariateCount, CovariateMean)
    colnames(nonOutTest)[-1] <- paste0('Test',colnames(nonOutTest)[-1], 'WithNoOutcome')
    nonOutTrain <- result %>% data.frame() %>%
      dplyr::filter( outcomeCount == 0 & index == F ) %>%
      dplyr::select(covariateId, CovariateCount, CovariateMean)
    colnames(nonOutTrain)[-1] <- paste0('Train',colnames(nonOutTrain)[-1], 'WithNoOutcome')
    outTest <- result %>% data.frame() %>%
      dplyr::filter( outcomeCount == 1 & index == T ) %>%
      dplyr::select(covariateId, CovariateCount, CovariateMean)
    colnames(outTest)[-1] <- paste0('Test',colnames(outTest)[-1], 'WithOutcome')
    outTrain <- result %>% data.frame() %>%
      dplyr::filter( outcomeCount == 1 & index == F) %>%
      dplyr::select(covariateId, CovariateCount, CovariateMean)
    colnames(outTrain)[-1] <- paste0('Train',colnames(outTrain)[-1], 'WithOutcome')
    
    covSummary <- as.data.frame(allResult %>%
                                  dplyr::full_join(nonOutTest , by ='covariateId') %>%
                                  dplyr::full_join(outTest , by ='covariateId') %>%
                                  dplyr::full_join(nonOutTrain , by ='covariateId') %>% 
                                  dplyr::full_join(outTrain , by ='covariateId'))
  } else{
    covSummary <- as.data.frame(allResult)
  }
  
  covSummary[is.na(covSummary)] <- 0
  
  
  # make covariateValue 0 if NA
  if('covariateValue'%in%colnames(covSummary)){
    covSummary$covariateValue[is.na(covSummary$covariateValue)] <- 0
  }
  
  return(covSummary)
}

characterize <- function(plpData, population, N=1){
  
  if(!missing(population)){
    plpData$covariateData$population <- tibble::as_tibble(population) %>% 
      dplyr::select(rowId, outcomeCount)
    on.exit(plpData$covariateData$population <- NULL, add = TRUE)
    # restrict to pop
    covariates <- plpData$covariateData$covariates %>% 
      dplyr::inner_join(plpData$covariateData$population)
  } else{
    covariates <- plpData$covariateData$covariates
  }
  
  
  popSize <- as.data.frame(covariates %>% dplyr::select(rowId) %>% 
                             dplyr::summarise(counts = dplyr::n_distinct(rowId)))$counts
  
  allPeople <- covariates %>% 
    dplyr::group_by(covariateId) %>%
    dplyr::summarise(CovariateCount = dplyr::n(),
                     CovariateFraction = 1.0*dplyr::n()/popSize) %>%
    dplyr::filter(CovariateCount >= N) %>%
    dplyr::select(covariateId, CovariateCount,CovariateFraction)
  allPeople <- as.data.frame(allPeople)
  
  return(allPeople)
}