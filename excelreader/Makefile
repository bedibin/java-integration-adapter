include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS =	.:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:servicemanagerreader.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/jms.jar:${JAR_DIR}/poi.jar:${JAR_DIR}/poi-ooxml.jar

JAR = excelreader.jar
DIR = classes

FILES = excelreader.java

all: $(JAR)

$(JAR): $(FILES) ../javaadapter/javaadapter.jar ../dbsyncplugin/dbsyncplugin.jar
	@mkdir -p $(DIR)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

run::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} dbsyncplugin excelreaderadapter.xml

clean::
	rm -Rf *.csv csv/*.csv ${JAR} ${DIR} *.log
