include ../Makefile.def

envl = LANG=fr_CA.iso88591 NLS_LANG=FRENCH_CANADA.WE8ISO8859P1

CLASS = ${JAR_DIR}/scheduling.jar:${JAR_DIR}/jms.jar:${JAR_DIR}/js.jar:${JAR_DIR}/json.jar

JAR =	javaadapter.jar
DIR =	classes

FILES =	javaadapter.java publish.java soap.java xml.java misc.java crypt.java base64coder.java ldap.java soapserverstandalone.java hook.java subscriber.java script.java csvwriter.java notrustsslsocket.java jms.java

JAVA_ARGS += -Dweblogic.security.TrustKeyStore=CustomTrust -Dweblogic.security.CustomTrustKeyStoreFileName=/opt/hpws/tomcat/conf/.keystore -Dweblogic.security.CustomTrustKeyStorePassPhrase=projetMGT -Djavaadapter.soap.nsenv="http://www.w3.org/2003/05/soap-envelope"
#JAVA_ARGS += -Djavax.xml.transform.TransformerFactory=oracle.xml.jaxp.JXSAXTransformerFactory
#JAVA_ARGS += -Djavax.xml.transform.TransformerFactory=org.apache.xalan.processor.TransformerFactoryImpl
#JAVA_ARGS += -Djavax.xml.transform.TransformerFactory=net.sf.saxon.TransformerFactoryImpl
#JAVA_ARGS += -verbose -Djava.endorsed.dirs=/opt/hpws/tomcat/common/endorsed

all:	${JAR}

$(JAR): $(FILES)
	@mkdir -p $(DIR)
	javac -d ${DIR} ${JAVAC_ARGS} -classpath ${CLASS} ${FILES}
	(cd ${DIR}; jar cvf ../${JAR} *.class)

clean::
	rm -Rf ${JAR} ${DIR}

run::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter

crypt::
	java ${JAVA_ARGS} -classpath ${JAR}:${CLASS} javaadapter crypt "${PASS}"

xml::
	xmllint --noout --schema javaadapter.xsd javaadapter.xml
