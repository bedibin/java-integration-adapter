include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = /app/bea/xng/v6.3.0/Xng_config:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/jms.jar:${JAR_DIR}/asi.jar:${JAR_DIR}/core-services-client-stubs.jar

JAR =	graniteadapter.jar
DIR =	classes

FILES =	graniteasi.java graniteasi_object.java granitequery.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar ../dbsyncplugin/dbsyncplugin.jar
	@mkdir -p $(DIR)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

clean::
	rm -Rf ${JAR} ${DIR}

run::
	rm -f granitequery.last
#	cp granitequery.info.test granitequery.info
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter graniteadapter.xml

debug::
	jswat

