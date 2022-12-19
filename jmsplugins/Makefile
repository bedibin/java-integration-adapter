include ../Makefile.def

CLASS = ../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/js.jar:${JAR_DIR}/json.jar
JAR = JNDITopic.jar MQQueue.jar
DIR = classes

JNDITopic.jar: CLASS := "${CLASS}:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar"
MQQueue.jar: CLASS := "${CLASS}:${JAR_DIR}/com.ibm.mq.allclient.jar:${JAR_DIR}/jms.jar"

all: ${JAR}

${DIR}/%.class: %.java
	@mkdir -p ${DIR}
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} $<

%.jar: ${DIR}/%.class
	(cd ${DIR}; jar cvf ../$@ $(basename $(notdir $<))*.class)

clean::
	rm -Rf ${JAR} ${DIR}
