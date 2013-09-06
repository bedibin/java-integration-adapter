<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
xmlns:javaadapter="http://schemas.hp.com/adapter"
>
<xsl:output method="xml" encoding="UTF-8" indent="yes"/>

<xsl:template name="writevalue">
	<xsl:param name="str"/>
	<xsl:choose>
		<xsl:when test="not($str)">
			<xsl:value-of select="concat(' ','')"/>
		</xsl:when>
		<xsl:when test="starts-with($str,'#')">
			<xsl:value-of select="concat('#',$str)"/>
		</xsl:when>
		<xsl:when test="starts-with($str,'~')">
			<xsl:value-of select="concat('#',$str)"/>
		</xsl:when>
		<xsl:when test="starts-with($str,'>')">
			<xsl:value-of select="concat('>',$str)"/>
		</xsl:when>
		<xsl:otherwise>
			<xsl:value-of select="$str"/>
		</xsl:otherwise>
	</xsl:choose>
</xsl:template>

<xsl:template name="makevalue">
	<xsl:param name="str"/>
	<xsl:param name="sep"/>
	<xsl:param name="name"/>

	<xsl:choose>
		<xsl:when test="starts-with($name,'LIST_') and contains($name,'-')">
			<xsl:element name="{substring-before(substring-after($name,'LIST_'),'-')}">
				<xsl:call-template name="makearray">
					<xsl:with-param name="str" select="$str"/>
					<xsl:with-param name="sep" select="$sep"/>
					<xsl:with-param name="name" select="substring-after($name,'LIST_')"/>
				</xsl:call-template>
				<xsl:call-template name="makearray">
					<xsl:with-param name="str" select="''"/>
					<xsl:with-param name="sep" select="$sep"/>
					<xsl:with-param name="name" select="substring-after($name,'LIST_')"/>
				</xsl:call-template>
			</xsl:element>
		</xsl:when>
		<xsl:when test="starts-with($name,'LIST_')">
			<xsl:element name="{substring-after($name,'LIST_')}">
				<xsl:call-template name="makearray">
					<xsl:with-param name="str" select="$str"/>
					<xsl:with-param name="sep" select="$sep"/>
					<xsl:with-param name="name" select="substring-after($name,'LIST_')"/>
				</xsl:call-template>
				<!-- <xsl:call-template name="makearray">
					<xsl:with-param name="str" select="''"/>
					<xsl:with-param name="sep" select="$sep"/>
					<xsl:with-param name="name" select="substring-after($name,'LIST_')"/>
				</xsl:call-template> -->
			</xsl:element>
		</xsl:when>
		<xsl:when test="contains($name,'-')">
			<xsl:element name="{substring-before($name,'-')}">
				<xsl:element name="{substring-after($name,'-')}">
					<xsl:call-template name="writevalue">
						<xsl:with-param name="str" select="$str"/>
					</xsl:call-template>
				</xsl:element>
			</xsl:element>
		</xsl:when>
		<xsl:otherwise>
			<xsl:element name="{$name}">
				<xsl:call-template name="writevalue">
					<xsl:with-param name="str" select="$str"/>
				</xsl:call-template>
			</xsl:element>
		</xsl:otherwise>
	</xsl:choose>

</xsl:template>

<xsl:template name="makearray">
	<xsl:param name="str"/>
	<xsl:param name="sep"/>
	<xsl:param name="name"/>
	<xsl:choose>
		<xsl:when test="contains($str,$sep)">
			<xsl:call-template name="makearray">
				<xsl:with-param name="str" select="substring-before($str,$sep)"/>
				<xsl:with-param name="sep" select="$sep"/>
				<xsl:with-param name="name" select="$name"/>
			</xsl:call-template>
			<xsl:call-template name="makearray">
				<xsl:with-param name="str" select="substring-after($str,$sep)"/>
				<xsl:with-param name="sep" select="$sep"/>
				<xsl:with-param name="name" select="$name"/>
			</xsl:call-template>
		</xsl:when>
		<xsl:when test="contains($name,'-')">
			<xsl:element name="{substring-before($name,'-')}">
				<xsl:element name="{substring-after($name,'-')}">
					<xsl:call-template name="writevalue">
						<xsl:with-param name="str" select="$str"/>
					</xsl:call-template>
				</xsl:element>
			</xsl:element>
		</xsl:when>
		<xsl:otherwise>
			<xsl:element name="{$name}">
				<xsl:call-template name="writevalue">
					<xsl:with-param name="str" select="$str"/>
				</xsl:call-template>
			</xsl:element>
		</xsl:otherwise>
	</xsl:choose>
</xsl:template>

<xsl:template match="ISMDatabaseUpdate">
<javaadapter:multi>
<xsl:for-each select="start">
	<start>
		<xsl:for-each select="../@*">
			<xsl:attribute name="{name()}"><xsl:value-of select="."/></xsl:attribute>
		</xsl:for-each>
		<start/>
	</start>
</xsl:for-each>
<xsl:for-each select="add | remove | update">
<xsl:variable name="operation" select="name()"/>
<xsl:variable name="opersm">
	<xsl:choose>
		<xsl:when test="$operation='add'">Create</xsl:when>
		<xsl:when test="$operation='remove'">Delete</xsl:when>
		<xsl:when test="$operation='update'">Update</xsl:when>
	</xsl:choose>
</xsl:variable>
<xsl:element name="{concat($opersm,../@name,'Request')}">
<model>
<keys>
	<xsl:for-each select="*">
		<xsl:if test="@type = 'key'">
			<xsl:element name="{name()}">
				<xsl:value-of select="./text()"/>
			</xsl:element>
		</xsl:if>
	</xsl:for-each>
</keys>
<instance>
	<xsl:for-each select="*">
		<xsl:if test="($operation = 'remove' and @type = 'key') or ($operation = 'add' and @type = 'initial') or ($operation != 'remove' and not(@type))">
			<xsl:call-template name="makevalue">
				<xsl:with-param name="str" select="text()"/>
				<xsl:with-param name="sep" select="'&#xA;'"/>
				<xsl:with-param name="name" select="name()"/>
			</xsl:call-template>
		</xsl:if>
	</xsl:for-each>
</instance>
</model>
</xsl:element>
</xsl:for-each>
<xsl:for-each select="end">
	<end>
		<xsl:for-each select="../@*">
			<xsl:attribute name="{name()}"><xsl:value-of select="."/></xsl:attribute>
		</xsl:for-each>
		<end/>
	</end>
</xsl:for-each>
</javaadapter:multi>
</xsl:template>
</xsl:stylesheet>
