include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = .:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/json.jar:${JAR_DIR}/jms.jar:${JAR_DIR}/servlet-api.jar

DIR =	classes
LIB =	WEB-INF/lib
JAR =	${LIB}/javaadapterservlet.jar
WAR =	javaadapter.war

FILES =	JavaAdapterServlet.java JavaAdapterListener.java

all:	${JAR} ${WAR}

${JAR}: ${FILES}
	@mkdir -p $(DIR) $(LIB)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

${WAR}: ${JAR}
	cp ../javaadapter/javaadapter.jar ${LIB}
	cp ${JAR_DIR}/scheduling.jar ${LIB}
	cp ../dbsyncplugin/dbsyncplugin.jar ${LIB}
	jar cvf ${WAR} WEB-INF/lib/*.jar

clean::
	rm -Rf ${JAR} ${WAR} ${DIR} WEB-INF

