include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS =	.:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:servicemanagerreader.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar:${JAR_DIR}/ojdbc14.jar:${JAR_DIR}/poi.jar:${JAR_DIR}/poi-ooxml.jar

JAR = excelreader.jar
DIR = classes

FILES = excelreader.java

all: $(JAR)

$(JAR): $(FILES) ../javaadapter/javaadapter.jar ../dbsyncplugin/dbsyncplugin.jar
	$(JAVA_HOME)/bin/javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; $(JAVA_HOME)/bin/jar cvf ../${JAR} *.class)

run::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} dbsyncplugin excelreaderadapter.xml

clean::
	rm -f *.csv csv/*.csv ${JAR} ${DIR}/*.class *.log
