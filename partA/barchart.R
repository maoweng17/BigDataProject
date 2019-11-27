library(ggplot2)

df = read.table('/Users/maoweng17/Documents/QMUL/BigDataProcessing/Lab/coursework/partA/out_parA.txt',sep='\t')
names(df) = c('date','value')
df = df [df$date != '1970-01',]

ggplot(data=df, aes(x=date, y=value)) +
  geom_bar(stat="identity", fill="steelblue")+
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) 
