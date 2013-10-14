DIR =	javaadapter \
	dbsyncplugin \
	queuedirplugin \
	servlet \
	assetmanager \
	ucmdb \
	servicemanager

all:	$(DIR)

$(DIR)::
	cd $@; $(MAKE) $(MAKEFLAG)

clean::
	@$(MAKE) MAKEFLAG="clean"

lint::
	@$(MAKE) MAKEFLAG="lint"


