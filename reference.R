library(Hmisc)
library(ggplot2)
library(doBy)
df <- data.frame(row=sample(c("A", "B"), 10, replace=T),
                 col=sample(c("x", "y"), 10, replace=T),
                 val=rnorm(10))
sdf <- summaryBy(val~row+col, data=df, FUN=mean)
ggplot(sdf, aes(x=row, y=col)) +
  geom_tile(aes(fill = val.mean), colour = "white") +
  scale_fill_gradient(low = "white", high = "yellow") + geom_text(aes(label="good"))

# data(khan)
# heatplot(khan$train[1:30,], lowcol="blue", highcol="red")