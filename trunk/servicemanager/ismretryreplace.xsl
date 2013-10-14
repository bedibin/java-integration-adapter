<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
xmlns:sm="http://schemas.hp.com/SM/7"
xmlns:javaadapter="http://schemas.hp.com/adapter"
>
<xsl:output method="xml" encoding="UTF-8" indent="yes"/>

<xsl:template match="/*">
<javaadapter:multi>
<xsl:variable name="elname">
	<xsl:choose>
		<xsl:when test="starts-with(name(),'Update')">
			<xsl:value-of select="substring-after(name(),'Update')"/>
		</xsl:when>
		<xsl:when test="starts-with(name(),'Create')">
			<xsl:value-of select="substring-after(name(),'Create')"/>
		</xsl:when>
		<xsl:when test="starts-with(name(),'Delete')">
			<xsl:value-of select="substring-after(name(),'Delete')"/>
		</xsl:when>
	</xsl:choose>
</xsl:variable>
<xsl:element name="{concat('Update',substring-before($elname,'Response'),'Request')}">
	<xsl:copy-of select="model"/>
</xsl:element>
</javaadapter:multi>
</xsl:template>
</xsl:stylesheet>

