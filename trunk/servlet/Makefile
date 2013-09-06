include ../../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = .:../javaadapter/javaadapter.jar:../dbsyncplugin/dbsyncplugin.jar:${JAR_DIR}/scheduling.jar:${JAR_DIR}/xercesImpl.jar:${JAR_DIR}/wljmsclient.jar:${JAR_DIR}/webserviceclient.jar:${JAR_DIR}/ojdbc14.jar:${JAR_DIR}/servlet-api.jar

DIR =	classes
LIB =	WEB-INF/lib
JAR =	${LIB}/javaadapterservlet.jar
WAR =	javaadapter.war

FILES =	JavaAdapterServlet.java JavaAdapterListener.java

all:	${JAR} ${WAR}

${JAR}: ${FILES}
	$(JAVA_HOME)/bin/javac -d ${DIR} -Xlint:unchecked -classpath ${CLASS} ${FILES}
	(cd ${DIR}; $(JAVA_HOME)/bin/jar cvf ../${JAR} *.class)

${WAR}: ${JAR}
	cp ../javaadapter/javaadapter.jar ${LIB}
	cp ${JAR_DIR}/scheduling.jar ${LIB}
	cp ../dbsyncplugin/dbsyncplugin.jar ${LIB}
	cp ${JAR_DIR}/xercesImpl.jar ${LIB}
	cp ../servicemanager/ticketupdatetoism.xsl WEB-INF
	cp ../servicemanager/newservicecreatetoism.xsl WEB-INF
	cp ../servicemanager/newserviceupdatetoism.xsl WEB-INF
	cp ../servicemanager/outageupdatetoism.xsl WEB-INF
	cp ../servicemanager/ismrts.xsl WEB-INF
	cp ../servicemanager/ismtocreateticket.xsl WEB-INF
	cp ../servicemanager/ismtoupdateticket.xsl WEB-INF
	cp ../servicemanager/ismgestionpbx.xsl WEB-INF
	cp ../servicemanager/ismgestionpbxprelookup.xsl WEB-INF
	cp ../servicemanager/ismretry.xsl WEB-INF
	cp ../servicemanager/granitevendor.sql WEB-INF
	cp ../servicemanager/granitesite.sql WEB-INF
	cp ../servicemanager/granitedevice.sql WEB-INF
	cp ../servicemanager/ovcapoint.sql WEB-INF
	cp ../servicemanager/granitedevicerelation.sql WEB-INF
	cp ../servicemanager/dumpgraniteservice.php WEB-INF
	cp ../inventoryquery/phpwrap WEB-INF
	mkdir -p WEB-INF/csv
	$(JAVA_HOME)/bin/jar cvf ${WAR} WEB-INF/*.xml WEB-INF/*.xsl WEB-INF/lib/*.jar WEB-INF/csv WEB-INF/*.sh WEB-INF/*.sql WEB-INF/*.php WEB-INF/phpwrap

clean::
	rm -f ${JAR} ${WAR} ${DIR}/*.class ${LIB}/*.jar WEB-INF/*.xsl WEB-INF/*.php WEB-INF/*.sql WEB-INF/phpwrap
	rm -Rf WEB-INF/csv

