include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = .:../javaadapter/javaadapter.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/json.jar:${JAR_DIR}/jms.jar:${JAR_DIR}/wildcard.jar:${JAR_DIR}/ojdbc14.jar

JAR =	dbsyncplugin.jar
DIR =	classes

FILES =	dbsyncplugin.java dbsql.java dbsyncreader.java dbsyncoper.java dbsynclookup.java dbsyncfields.java dbsyncsync.java

all:	${JAR}

$(JAR): $(FILES) ../javaadapter/javaadapter.jar
	@mkdir -p $(DIR)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

clean::
	rm -Rf ${JAR} ${DIR}

run::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} dbsyncplugin dbsyncplugin.xml

test::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} dbsql dbsql.xml

server::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter dbsyncplugin.xml

debug::
	jswat

