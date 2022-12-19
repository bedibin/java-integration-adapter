include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = .:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/json.jar:${JAR_DIR}/jms.jar:$(JAR_DIR)/am-constants-98.jar:$(JAR_DIR)/am-jni-98.jar:${JAR_DIR}/commons-logging.jar

JAR =	assetmanagerdbsync.jar
DIR =	classes

FILES =	assetmanagerdbsync.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar ../dbsyncplugin/dbsyncplugin.jar
	@mkdir -p $(DIR)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

clean::
	rm -Rf ${JAR} ${DIR} hs_err_pid*.log

run::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter

test::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} assetmanagerdbsync

debug::
	jswat

