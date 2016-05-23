# When building through the build system,
# these are defined in the RPM spec file;
# If building locally, everything is put
# under './smapi_v3_build/'
# N.B.: paths do not contain '/'
#       in the beginning or end
DESTDIR ?= smapi_v3_build
SM_PLUGINS_DIR ?= $(DESTDIR)/usr/libexec/xapi-storage-script
LIBS_DIR ?= $(DESTDIR)/usr/lib/python2.7/site-packages/xapi/storage/libs

DATAPATH := datapath
VOLUME := volume
LIBS := libs
OVERLAY := overlay

SM_PLUGIN_TYPES := $(DATAPATH) $(VOLUME)
SM_PLUGINS := \
    $(foreach sm_plugin_type,$(SM_PLUGIN_TYPES),\
        $(shell find $(sm_plugin_type)/* -maxdepth 0 -type d) \
    )

# All datapaths listed here will be
# symlinks to the tapdisk/ directory
TAPDISK_SYMLINKS := \
    raw+file \
    vhd+file

# These variables hold all output files
FILES_OUT :=
SYMLINKS_OUT :=

# Files created in the source directory
# that need to be cleaned up
CLEAN :=

LIB_FILES := $(shell find $(LIBS)/* -type f)
OVERLAY_FILES := $(shell find $(OVERLAY)/* -type f)

$(LIBS_DIR)/%: $(LIBS)/%
	$(install-recipe)

$(DESTDIR)/%: $(OVERLAY)/%
	$(install-recipe)

FILES_OUT += \
    $(subst $(LIBS),$(LIBS_DIR),$(LIB_FILES)) \
    $(subst $(OVERLAY),$(DESTDIR),$(OVERLAY_FILES))

$(foreach sm_plugin,$(SM_PLUGINS), \
    $(eval $(call make-sm_plugin-rules,$(sm_plugin))) \
)

.PHONY: install_files
install_files: $(FILES_OUT)

.PHONY: make_symlinks
make_symlinks: $(SYMLINKS_OUT)
	for link in $(TAPDISK_SYMLINKS); do \
		$(SYMLINK) tapdisk $(SM_PLUGINS_DIR)/$(DATAPATH)/$$link; \
	done

.PHONY: install
install: install_files make_symlinks

.PHONY: clean
clean:
	rm -rf $(CLEAN) $(DESTDIR)
