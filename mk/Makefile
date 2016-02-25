USE_BRANDING := yes
IMPORT_BRANDING := yes
IMPORT_VERSIONS := yes
REPONAME := xapi-storage-plugins

include $(B_BASE)/common.mk
include $(B_BASE)/rpmbuild.mk

-include $(MY_OBJ_DIR)/version.inc
.PHONY: $(MY_OBJ_DIR)/version.inc
$(MY_OBJ_DIR)/version.inc:
	$(version-makefile) > $@
	$(call git_cset_number,$(REPONAME)) >> $@
	echo XAPI_STORAGE_PLUGINS_VERSION := $(shell cat ../VERSION) >> $@
	echo XAPI_STORAGE_PLUGINS_RELEASE := xs\$$\(CSET_NUMBER\) >> $@

XAPI_STORAGE_PLUGINS_SPEC := xapi-storage-plugins.spec
XAPI_STORAGE_PLUGINS_SRC := $(RPM_SOURCESDIR)/xapi-storage-plugins-$(XAPI_STORAGE_PLUGINS_VERSION).tar.bz2
XAPI_STORAGE_PLUGINS_SRPM := xapi-storage-plugins-$(XAPI_STORAGE_PLUGINS_VERSION)-$(XAPI_STORAGE_PLUGINS_RELEASE).src.rpm
XAPI_STORAGE_PLUGINS_STAMP := $(MY_OBJ_DIR)/.rpmbuild.stamp
REPO := $(call git_loc,xapi-storage-plugins)


.PHONY: build
build: sources $(XAPI_STORAGE_PLUGINS_STAMP)

$(MY_SOURCES)/MANIFEST: $(RPM_SRPMSDIR)/$(XAPI_STORAGE_PLUGINS_SRPM) $(MY_SOURCES_DIRSTAMP) $(RPM_BUILD_COOKIE)
	rm -f $@
	/bin/sh ./srpms-to-manifest $(COMPONENT) $(MY_OUTPUT_DIR)/SRPMS > $@

.PHONY: sources
sources: $(MY_SOURCES)/MANIFEST

.PHONY: clean
clean:
	rm -f $(XAPI_STORAGE_PLUGINS_STAMP) $(XAPI_STORAGE_PLUGINS_SRC) $(RPM_SPECSDIR)/$(XAPI_STORAGE_PLUGINS_SPEC)

.SECONDARY: $(XAPI_STORAGE_PLUGINS_SRC)
$(XAPI_STORAGE_PLUGINS_SRC): $(XAPI_STORAGE_PLUGINS_SOURCES)
	echo XAPI_STORAGE_PLUGINS_VERSION $(XAPI_STORAGE_PLUGINS_VERSION) PLATFORM_VERSION $(PLATFORM_VERSION)
	git --git-dir $(REPO)/.git archive --prefix=xapi-storage-plugins-$(XAPI_STORAGE_PLUGINS_VERSION)/ \
		--format=tar HEAD | bzip2 > $(RPM_SOURCESDIR)/xapi-storage-plugins-$(XAPI_STORAGE_PLUGINS_VERSION).tar.bz2

.SECONDARY: $(RPM_SPECSDIR)/%.spec
$(RPM_SPECSDIR)/%.spec: *.spec.in
	sed -e 's/@XAPI_STORAGE_PLUGINS_VERSION@/$(XAPI_STORAGE_PLUGINS_VERSION)/g' \
          -e 's/@XAPI_STORAGE_PLUGINS_RELEASE@/$(XAPI_STORAGE_PLUGINS_RELEASE)/g' \
          < $< \
          > $@

$(RPM_SRPMSDIR)/$(XAPI_STORAGE_PLUGINS_SRPM): $(RPM_DIRECTORIES) $(RPM_SPECSDIR)/$(XAPI_STORAGE_PLUGINS_SPEC) $(XAPI_STORAGE_PLUGINS_SRC)
	$(RPMBUILD) -bs $(RPM_SPECSDIR)/$(XAPI_STORAGE_PLUGINS_SPEC)

$(XAPI_STORAGE_PLUGINS_STAMP): $(RPM_SRPMSDIR)/$(XAPI_STORAGE_PLUGINS_SRPM)
	$(RPMBUILD) --rebuild --target $(DOMAIN0_ARCH_OPTIMIZED) $(RPM_SRPMSDIR)/$(XAPI_STORAGE_PLUGINS_SRPM)
	mkdir -p $(MY_MAIN_PACKAGES)
	cp $(RPM_RPMSDIR)/$(DOMAIN0_ARCH_OPTIMIZED)/xapi-storage-plugins-*.rpm $(MY_MAIN_PACKAGES)
	# Deliberately omit the debuginfo RPM (xapi-storage-plugins-debuginfo-...)
	rm -f $(MY_MAIN_PACKAGES)/xapi-storage-plugins-debuginfo-*.rpm
	touch $@
