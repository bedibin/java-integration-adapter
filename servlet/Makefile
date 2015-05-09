include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = .:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar:${JAR_DIR}/ojdbc14.jar:${JAR_DIR}/servlet-api.jar

DIR =	classes
LIB =	WEB-INF/lib
JAR =	${LIB}/javaadapterservlet.jar
WAR =	javaadapter.war

FILES =	JavaAdapterServlet.java JavaAdapterListener.java

all:	${JAR} ${WAR}

${JAR}: ${FILES}
	$(JAVA_HOME)/bin/javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; $(JAVA_HOME)/bin/jar cvf ../${JAR} *.class)

${WAR}: ${JAR}
	cp ../javaadapter/javaadapter.jar ${LIB}
	cp ${JAR_DIR}/scheduling.jar ${LIB}
	cp ../dbsyncplugin/dbsyncplugin.jar ${LIB}
	$(JAVA_HOME)/bin/jar cvf ${WAR} WEB-INF/lib/*.jar

clean::
	rm -f ${JAR} ${WAR} ${DIR}/*.class ${LIB}/*.jar WEB-INF/*.xsl WEB-INF/*.php WEB-INF/*.sql WEB-INF/phpwrap
	rm -Rf WEB-INF/csv

