include ../Makefile.def

CLASS = ../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/js.jar:${JAR_DIR}/json.jar
JAR = PGPcrypto.jar DPAPIcrypto.jar SecretSRVcrypto.jar
DIR = classes

PGPCLASS = ${JAR_DIR}/bcprov-jdk15on-170.jar:${JAR_DIR}/bcpg-jdk15on-170.jar
DPAPICLASS = ${JAR_DIR}/jdpapi-java-1.0.1.jar
HTTPCLIENTCLASS = ${JAR_DIR}/httpclient5-win-5.1.3.jar:${JAR_DIR}/httpclient5-5.1.3.jar:${JAR_DIR}/httpcore5-5.1.3.jar:${JAR_DIR}/gson.jar:${JAR_DIR}/slf4j-api.jar
PGPcrypto.jar: CLASS := "${CLASS}:${PGPCLASS}"
DPAPIcrypto.jar: CLASS := "${CLASS}:${DPAPICLASS}"
SecretSRVcrypto.jar: CLASS := "${CLASS}:${HTTPCLIENTCLASS}"

all: ${JAR}

${DIR}/%.class: %.java
	@mkdir -p ${DIR}
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} $<

%.jar: ${DIR}/%.class
	(cd ${DIR}; jar cvf ../$@ $(basename $(notdir $<))*.class)

run::
	java -cp ${CLASS}:${PGPCLASS}:${DPAPICLASS}:${HTTPCLIENTCLASS}:PGPcrypto.jar:DPAPIcrypto.jar:SecretSRVcrypto.jar dbsyncplugin javaadapter.xml

clean::
	rm -Rf ${JAR} ${DIR}
