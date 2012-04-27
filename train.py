import base64
import md5
import os.path
import sys
import time
import csv
import md5
import os

if (len(sys.argv) < 2):
    print "train.py train.csv modelName "
    sys.exit()

args = sys.argv;
reader = csv.reader(open(args[1],'rb'), delimiter=',')

model = sys.argv[2];


#find name of directory which is not normal
path = "./"+model
listing = os.listdir(path);

actual = "";
for item in listing:
    if item != 'normal':
        actual=item
        break;

if (actual == ""):
    print "The model was not found"
    sys.exit(1);

category = actual

def whetherExistsInNormal(model,url):
    name =  base64.b64encode(url)
    return os.path.isfile(model+"/normal/"+name);

def whetherExistsInActual(model_path,url,model):
    name =  base64.b64encode(url)
    return os.path.isfile(model_path+"/"+model+"/"+name);

def deleteFromNormal(model,item):
     name = base64.b64encode(item)
     os.remove(model+"/normal/"+name)

def deleteFromActual(model,modelName,item):
    name = base64.b64encode(item)
    os.remove(model+"/"+modelName+"/"+name)

toPutInActual = []
toPutInNormal = []
filesToDelete = []
for row in reader:
    if (len(row) < 2):
        continue
    flag = row[1];
    #list of files to delete
    m=md5.new()
    m.update(item)
    name =m.hexdigest()
    if (os.path.exists(model+"/"+category+"/"+name) or os.path.exists(model+"/normal/"+name)):
        filesToDelete.append(name)
        
    whetherCorrect = (flag == 'true')
    if (whetherCorrect==True and (False == whetherExistsInActual(path,row[0],model))):
        toPutInActual.append(row[0]);

    if (whetherCorrect==False and (False == whetherExistsInNormal(path,row[0]))):
        toPutInNormal.append(row[0]);
        

#delete files

for file in filesToDelete:
    os.remove(model+"/normal/"+file);
    os.remove(model+"/"+category+"/"+file);


#files to create in normal
m = md5.new()
m.update(str(time.time()))
hash = m.hexdigest()
normalWriter = csv.writer(open('./tmp/'+hash+'1.csv','wb'))


for row in toPutInActual:
    normalWriter.writerow([row])

normalWriter = csv.writer(open('./tmp/'+hash+'2.csv','wb'))

for row in toPutInNormal:
    normalWriter.writerow([row])

#feed each csv to the retrainer application
os.system('java -jar trainer.jar tmp/'+hash+'1.csv '+model+'/'+category)
os.system('java -jar trainer.jar tmp/'+hash+'2.csv '+model+'/normal')


#prepare 20 newsgroups
os.system('export JAVA_HOME=/usr/lib/jvm/default-java/')
os.system('mahout prepare20newsgroups -p '+model+' -o tmp/'+hash+' -a org.apache.mahout.vectorizer.DefaultAnalyzer -c UTF-8')
os.system('hadoop dfs -mkdir model_staging')
os.system('hadoop dfs -put tmp/'+hash+' model_staging');
os.system('hadoop dfs -mkdir model_output');
os.system('hadoop dfs -rmr model_output/'+model);
os.system('mahout trainclassifier -i model_staging/'+hash+' -o model_output/'+model+' -type bayes -ng 1 -source hdfs')
