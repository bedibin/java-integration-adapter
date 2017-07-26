include ../Makefile.def

CLASS = .:../javaadapter/javaadapter.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/json.jar:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar
JAR =	queuedirplugin.jar
DIR =	classes

FILES =	queuedirplugin.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar
	$(JAVA_HOME)/bin/javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; $(JAVA_HOME)/bin/jar cvf ../${JAR} *.class)

clean::
	rm -f ${JAR} ${DIR}/*.class

run::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter queuedirplugin.xml

debug::
	jswat

