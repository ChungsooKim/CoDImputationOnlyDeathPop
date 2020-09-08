#' Execute the Imputation study
#' @NAME causeImputation
#' @details This function will run the random forest model for classify causes of death
#' @import dplyr 
#' @import ROCR
#' @import pROC
#' @import caret
#' @import ggplot2
#' @import xlsx
#' @import rJava
#' @import ParallelLogger
#'
#' @param TAR                  Time at risk for determining risk window
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/)
#' @export


causeImputation <- function(outputFolder, TAR){
  
  `%notin%` <- Negate(`%in%`)
  
  saveFolder <- file.path(outputFolder ,"CoDImputationOnlyDeathPop")
  if (!file.exists(saveFolder))
    dir.create(saveFolder, recursive = T)
  
  savepath <- file.path(saveFolder,"out_df_")
  savepath <- paste(savepath, TAR,".rds", sep = "")
  outDF <- readRDS(savepath)
  
  ParallelLogger::logInfo("Imputation Start...")
  ParallelLogger::logInfo("Read Imputation table file in save folder...")
  
  ### Run Imputation
  ParallelLogger::logInfo("Read the cause prediction model...")
  modelpath <- paste(getwd(), "inst", "finalModels", "final_model", sep = "/")
  modelpath <- paste(modelpath, TAR, sep = "_")
  modelpath <- paste(modelpath, "rds", sep = ".")
  cause.model <- readRDS(modelpath)
  
  ### Result
  ParallelLogger::logInfo("Predicting response and calculating prediction values...")
  dataTest <- outDF
  
  dataTestResult <- dataTest
  dataTestResult$cause.prediction <- predict(cause.model, dataTest)
  #dataTestResult$cause.value <- predict(cause.model, dataTest, type = "prob")
  
  dataTestResult <- dataTestResult %>% select(1:24, cause.prediction)
  
  
  
  
  ## Result file
  causeName <- data.frame(cause.prediction = c(1:7,99), 
                          CauseName = c("Malignant cancer", "Heart disease", 
                                        "Cerebrovascular disease","Pneumonia", "Diabetes", 
                                        "Liver disease", "Chronic lower respiratory disease", 
                                        "Others"))
  causeName$cause.prediction <- as.factor(causeName$cause.prediction)
  
  totalN <- nrow(dataTestResult)
  deathN <- nrow(dataTestResult[dataTestResult$cause.prediction!=0,])
  
  table1 <- dataTestResult %>%
    group_by(cause.prediction) %>%
    summarise(
      Count = n(), 
      "Percent(%)" = round(n()/totalN*100,2), 
      "Percent in Death(%)" = round(n()/deathN*100,2)
    )
  
  table1 <- merge(causeName, table1, by = "cause.prediction", all.x = T) 
  table1[is.na(table1)] <- 0
  
  temp <- table1[1,] %>% mutate(cause.prediction = "-", CauseName ="total N of target cohort", Count = totalN, 
                                "Percent(%)" = totalN/totalN*100, "Percent in Death(%)" = "-")
  
  table1 <- rbind(temp, table1)
  
  table1 <- table1 %>%
    arrange(desc(`Percent(%)`)) %>%
    select(-cause.prediction)
  
  # Cause of death by Year
  # table2 - raw counts, table3 - percent in death, table4 - rank by year
  
  table2 <- dataTestResult %>%
    group_by(cohortStartDate, cause.prediction) %>%
    summarise(personCount = n())
  
  dataTestResult$cohortStartDate <- as.factor(dataTestResult$cohortStartDate)
  lengthYear <- length(levels(dataTestResult$cohortStartDate))
  
  personCountYearly <- dataTestResult %>%
    group_by(cohortStartDate) %>%
    summarise(personCountYearly = n())
  
  deathCountYearly <- dataTestResult[dataTestResult$cause.prediction != 0,] %>%
    group_by(cohortStartDate) %>%
    summarise(deathCountYearly = n())
  deathCountYearly <- merge(personCountYearly, deathCountYearly, by = "cohortStartDate", all.x = T)
  deathCountYearly[is.na(deathCountYearly)] <- 0
  
  temp <- data.frame(cause.prediction = c(1:7,99))
  table2 <- reshape2::dcast(table2, cause.prediction ~ cohortStartDate, value.var = "personCount")
  table2 <- merge(temp, table2, by = "cause.prediction", all.x = T)
  table2[is.na(table2)] <- 0
  table2 <- table2 %>% mutate(Total = rowSums(table2[2:length(table2)]))
  
  
  table3 <- table2 %>%
    filter(cause.prediction != 0)
  
  table2 <- merge(causeName, table2, by = "cause.prediction", all.y = T) %>% select(-cause.prediction)
  table2[9,2:length(table2)] <-table2[2:length(table2)] %>% summarise_all(funs(sum))
  levels(table2$CauseName) <- c(levels(table2$CauseName), "Total")
  table2$CauseName[9] <- "Total"
  
  for (i in 1:lengthYear){
    table3[,i+1] <- round(table3[,i+1]/rep(deathCountYearly$deathCountYearly[i], 
                                           times = length(levels(causeName$cause.prediction))-1)*100,2)  
  }
  
  table3[,lengthYear+2] <- round(table3[,lengthYear+2]/deathN*100,2)
  
  table3[is.na(table3)] <- "-"
  table3 <- merge(causeName[causeName$CauseName!="No Death",], 
                  table3, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  
  table4 <- table3

  for(i in 1:lengthYear){
    temp <- table4 %>%
      select(CauseName, levels(dataTestResult$cohortStartDate)[i])
    temp <- temp %>% arrange(desc(temp[,2]))
    table4[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table4 <- table4 %>% mutate(Rank = c(1:8)) %>% select (Rank, -CauseName, 2:lengthYear+1, -Total)

  #Age groups in 5 years
  #table 5 - row counts by age group in 5 years
  #table 6 - percent in death by age group in 5 years
  #table 7 - rank by age group in 5 year
  
  
  temp <- data.frame(cause.prediction = c(1:7,99))
  
  for(j in 1:20){
      temp2 <- dataTestResult[dataTestResult[j+4] == 1,]
      temp2 <- temp2 %>% 
        group_by(cause.prediction) %>%
        summarise(personCount = n()) %>% group_by()
      colnames(temp2)[2] <- colnames(dataTestResult[j+4])
    temp <- merge(temp, temp2, by = "cause.prediction", all.x = T)
    temp[is.na(temp)] <- 0
  }
  
  temp <- temp %>% select(cause.prediction, paste(seq(3, 19003, by = 1000)))
  table5 <- rename(temp, "0-4"="3", "5-9"="1003", "10-14"="2003", 
                   "15-19"="3003", "20-24"="4003", "25-29"="5003", 
                   "30-34"="6003", "35-39"="7003", "40-44"="8003", "45-49"="9003",
                   "50-54"="10003","55-59"="11003","60-64"="12003","65-69"="13003",
                   "70-74"="14003","75-79"="15003","80-84"="16003","85-89"="17003",
                   "90-94"="18003", "95-99"="19003")
  
  table5 <- table5 %>% mutate(Total = rowSums(table5[2:length(table5)]))
  
  table6 <- table5 %>% filter (cause.prediction != 0)
  
  table5 <- merge(causeName, table5, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  table5[9,2:length(table5)] <-table5[2:length(table5)] %>% summarise_all(funs(sum))
  levels(table5$CauseName) <- c(levels(table5$CauseName), "Total")
  table5$CauseName[9] <- "Total"
  
  deathCountAge <- colSums(table6)[2:21] 
 
   for(i in 1:20){
    table6[,i+1] <- round(table6[,i+1]/deathCountAge[i]*100 ,2)
  }
  table6 <- merge(causeName[causeName$CauseName!="No Death",],
                  table6, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  table6[is.na(table6)] <- "-"
  table6[,22] <- round(table6[,22]/deathN*100,2)
  
  table7 <- table6
  
  for(i in 1:20){
    temp <- table7 %>%
      select(CauseName, i+1)
    temp <- temp %>% arrange(desc(temp[,2]))
    temp[is.na(temp)] <- "-"
    table7[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table7 <- table7 %>% mutate(Rank = c(1:8)) %>% select (Rank, 2:21)
  
  
  #Age groups in 10 years
  #table 8 - row counts by age group in 5 years
  #table 9 - percent in death by age group in 5 years
  #table 10 - rank by age group in 5 year
  
  table8 <- data.frame(cause.prediction = c(1:7,99))
  for(i in 1:10){
    table8[i+1] <- table5[2*i]+table5[2*i+1]
    colnames(table8)[i+1] <- paste0(strsplit(colnames(table5[2*i]),"-")[[1]][1],
                                    "-", strsplit(colnames(table5[2*i+1]),"-")[[1]][2])
  }
  table8 <- table8 %>% mutate(Total = rowSums(table8[2:length(table8)]), )

  table9 <- table8 %>% filter(cause.prediction != 0)
  
  table8 <- merge(causeName, table8, by = "cause.prediction", all.y = T) %>% select(-cause.prediction)
  table8[9,2:length(table8)] <-table8[2:length(table8)] %>% summarise_all(funs(sum))
  levels(table8$CauseName) <- c(levels(table8$CauseName), "Total")
  table8$CauseName[9] <- "Total"
  
  deathCountAge10 <- colSums(table9[2:11])
  
  for(i in 1:10){
    table9[,i+1] <- round(table9[,i+1]/deathCountAge10[i]*100 ,2)
  }
  table9 <- merge(causeName[causeName$CauseName!="No Death",], table9, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  table9[is.na(table9)] <- "-"
  table9[,12] <- round(table9[,12]/totalN*100,2)
  
  table10 <- table9
  
  for(i in 1:10){
    temp <- table10 %>%
      select(CauseName, i+1)
    temp <- temp %>% arrange(desc(temp[,2]))
    temp[is.na(temp)] <- "-"
    table10[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table10 <- table10 %>% mutate(Rank = c(1:8)) %>% select (Rank, 2:11)
  
  
  
  # Cause of death by Gender
  # table11 - raw counts, table12 - percent in death, table13 - rank by gender
  table11 <- dataTestResult %>%
    group_by(`8532001`, cause.prediction) %>%
    summarise(personCount = n())
  colnames(table11) <- c("Female", "cause.prediction", "personCount")
  
  personCountByGender <- dataTestResult %>%
    group_by(`8532001`) %>%
    summarise(personCountByGender = n())
  colnames(personCountByGender) <- c("Female", "personCountByGender")
  
  deathCountByGender <- dataTestResult[dataTestResult$cause.prediction != 0,] %>%
    group_by(`8532001`) %>%
    summarise(deathCountByGender = n())
  colnames(deathCountByGender) <- c("Female", "deathCountByGender")
  
  deathCountByGender <- merge(personCountByGender, deathCountByGender, by = "Female", all.x = T)
  deathCountByGender[is.na(deathCountByGender)] <- 0
  
  
  temp <- data.frame(cause.prediction = c(1:7,99))
  table11 <- reshape2::dcast(table11, cause.prediction ~ Female, value.var = "personCount")
  colnames(table11) <- c("cause.prediction", "Male", "Female")
  table11 <- merge(temp, table11, by = "cause.prediction", all.x = T)
  table11[is.na(table11)] <- 0
  table11 <- table11 %>% mutate(Total = rowSums(table11[2:length(table11)]))
  
  table12 <- table11 %>%
    filter(cause.prediction != 0)
  
  table11 <- merge(causeName, table11, by = "cause.prediction", all.y = T) %>% select(-cause.prediction)
  table11[9,2:length(table11)] <-table11[2:length(table11)] %>% summarise_all(funs(sum))
  levels(table11$CauseName) <- c(levels(table11$CauseName), "Total")
  table11$CauseName[9] <- "Total"
  
  for (i in 1:2){
    table12[,i+1] <- round(table12[,i+1]/rep(deathCountByGender$deathCountByGender[i], 
                                             times = length(levels(causeName$cause.prediction))-1)*100,2)  
  }
  
  table12 <- merge(causeName[causeName$CauseName!="No Death",], 
                   table12, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  
  table12[,4] <- round(table12[,4]/deathN*100,2)
  
  table13 <- table12
  gender <- data.frame(gender = c("Male", "Female"))
  levels(gender$gender) <- c("Male", "Female")
  
  for(i in 1:2){
    temp <- table13 %>%
      select(CauseName, levels(gender$gender)[i])
    temp <- temp %>% arrange(desc(temp[,2]))
    table13[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table13 <- table13 %>% mutate(Rank = c(1:8)) %>% select (Rank, -CauseName, Male, Female, -Total)
  
  

  ### Save files in saveFolder
  ParallelLogger::logInfo("Saving the results...")
  
  saveName <- "CoDImputationResult"
  saveName <- paste0(saveName, ".xlsx")
  savepath <- file.path(saveFolder, saveName)
  xlsx::write.xlsx(table1, file = savepath, sheetName = "totalResult", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table2, file = savepath, sheetName = "CountByYear", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table3, file = savepath, sheetName = "PercentByYear", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table4, file = savepath, sheetName = "RankByYear", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table5, file = savepath, sheetName = "CountByAge5", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table6, file = savepath, sheetName = "PercentByAge5", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table7, file = savepath, sheetName = "RankByAge5", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table8, file = savepath, sheetName = "CountByAge10", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table9, file = savepath, sheetName = "PercentByAge10", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table10, file = savepath, sheetName = "RankByAge10", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table11, file = savepath, sheetName = "CountByGender", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table12, file = savepath, sheetName = "PercentByGender", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table13, file = savepath, sheetName = "RankByGender", col.names = T, row.names = F, append = T)

  ParallelLogger::logInfo("Done")
  
}

