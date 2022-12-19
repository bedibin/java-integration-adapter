include ../Makefile.def

CLASS = .:../javaadapter/javaadapter.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/json.jar:${JAR_DIR}/jms.jar
JAR =	queuedirplugin.jar
DIR =	classes

FILES =	queuedirplugin.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar
	@mkdir -p $(DIR)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

clean::
	rm -Rf ${JAR} ${DIR}

run::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter queuedirplugin.xml

debug::
	jswat

