include ../../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

JNI_JAR=$(shell ls /app/ovam/AssetManager/websvc/lib/am-jni-*.jar /gisti/AssetManager/websvc/lib/am-jni-*.jar)
CNST_JAR=$(shell ls /app/ovam/AssetManager/websvc/lib/am-constants-*.jar /gisti/AssetManager/websvc/lib/am-constants-*.jar)

CLASS = .:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/xercesImpl.jar:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar:${JAR_DIR}/ojdbc14.jar:$(JNI_JAR):$(CNST_JAR):${JAR_DIR}/commons-logging.jar

JAR =	assetmanagerdbsync.jar
DIR =	classes

FILES =	assetmanagerdbsync.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar ../dbsyncplugin/dbsyncplugin.jar
	$(JAVA_HOME)/bin/javac -d ${DIR} -Xlint:unchecked -classpath ${CLASS} ${FILES}
	(cd ${DIR}; $(JAVA_HOME)/bin/jar cvf ../${JAR} *.class)

clean::
	rm -f ${JAR} ${DIR}/*.class hs_err_pid*.log

run::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter

test::
	$(JAVA_HOME)/bin/java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} assetmanagerdbsync

debug::
	jswat

