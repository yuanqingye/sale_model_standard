library(dplyr,warn.conflicts = FALSE)
library(ggplot2,warn.conflicts = FALSE)
library(RSSL)
set.seed(2)
df <- generate2ClassGaussian(200, d=2, var = 0.2, expected=TRUE)

# Randomly remove labels
df <- df %>% add_missinglabels_mar(Class~.,prob=0.98) 

# Train classifier
g_nm <- NearestMeanClassifier(Class~.,df,prior=matrix(0.5,2))
g_self <- SelfLearning(Class~.,df,
                       method=NearestMeanClassifier,
                       prior=matrix(0.5,2))

# Plot dataset
df %>% 
  ggplot(aes(x=X1,y=X2,color=Class,size=Class)) +
  geom_point() +
  coord_equal() +
  scale_size_manual(values=c("-1"=3,"1"=3), na.value=1) +
  geom_linearclassifier("Supervised"=g_nm,
                        "Semi-supervised"=g_self)

# Evaluate performance: Squared Loss & Error Rate
mean(loss(g_nm,df))
mean(loss(g_self,df))


mean(predict(g_nm,df)!=df$Class)
mean(predict(g_self,df)!=df$Class)

library(RSSL)
set.seed(1)
# Set the datasets and corresponding formula objects
datasets <- list("2 Gaussian Expected"=
                   generate2ClassGaussian(n=2000,d=2,expected=TRUE),
                 "2 Gaussian Non-Expected"=
                   generate2ClassGaussian(n=2000,d=2,expected=FALSE))
formulae <- list("2 Gaussian Expected"=formula(Class~.),
                 "2 Gaussian Non-Expected"=formula(Class~.))
# Define the classifiers to be used
classifiers <- list("Supervised" =
                      function(X,y,X_u,y_u) { LeastSquaresClassifier(X,y)},
                    "Self-learning" =
                      function(X,y,X_u,y_u) { SelfLearning(X,y,X_u,
                                                           method = LeastSquaresClassifier)})
# Define the performance measures to be used and run experiment
measures <- list("Error" = measure_error, "Loss" = measure_losstest)
results_lc <- LearningCurveSSL(formulae,datasets,
                               classifiers=classifiers,
                               measures=measures,verbose=FALSE,
                               repeats=100,n_l=10,sizes = 2^(1:10))

library(RSSL)
library(dplyr)
library(ggplot2)
plot_style <- theme_classic() # Set the style of the plot
set.seed(2)
df_unlabeled <- generateCrescentMoon(n=100,sigma = 0.3) %>%
  add_missinglabels_mar(Class~.,prob=1)
df_labeled <- generateCrescentMoon(n=1,sigma = 0.3)
df <- rbind(df_unlabeled,df_labeled)
c_svm <- SVM(Class~.,df_labeled,scale=FALSE,
             kernel = kernlab::rbfdot(0.05),
             C=2500)
c_lapsvm1 <- LaplacianSVM(Class~.,df,scale=FALSE,
                          kernel=kernlab::rbfdot(0.05),
                          lambda = 0.0001,gamma=10)
c_lapsvm2 <- LaplacianSVM(Class~.,df,scale=FALSE,
                          kernel=kernlab::rbfdot(0.05),
                          lambda = 0.0001,gamma=10000)
# Plot the results
# Change the arguments of stat_classifier to plot the Laplacian SVM
ggplot(df_unlabeled, aes(x=X1,y=X2)) +
  geom_point() +
  geom_point(aes(color=Class,shape=Class),data=df_labeled,size=5) +
  stat_classifier(classifiers=list("SVM"=c_lapsvm2),color="black") +
  ggtitle("SVM")+
  plot_style


# Generate Example
df <- generate2ClassGaussian(n=1000, d=2, expected=FALSE)
df_semi <- add_missinglabels_mar(df, Class~., prob=0.995)
# Train and evaluate classifiers
mean(loss(LeastSquaresClassifier(Class~.,df_semi),df))
mean(loss(SelfLearning(Class~.,df_semi,method=LeastSquaresClassifier),df))
mean(loss(ICLeastSquaresClassifier(Class~.,df_semi),df))
mean(loss(ICLeastSquaresClassifier(Class~.,df_semi,
                                   projection="semisupervised"),df))
## [1] 0.1763921
## [1] 0.4813863
## [1] 0.1185772
## [1] 0.1236701