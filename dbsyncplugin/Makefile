include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = .:../javaadapter/javaadapter.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar:${JAR_DIR}/wildcard.jar:${JAR_DIR}/ojdbc14.jar

JAR =	dbsyncplugin.jar
DIR =	classes

FILES =	dbsyncplugin.java dbsql.java dbsyncreader.java dbsyncoper.java dbsynclookup.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar
	$(JAVA_HOME)/bin/javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; $(JAVA_HOME)/bin/jar cvf ../${JAR} *.class)

clean::
	rm -f ${JAR} ${DIR}/*.class

run::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} dbsyncplugin dbsyncplugin.xml

test::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} dbsql dbsql.xml

server::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter dbsyncplugin.xml

debug::
	jswat

