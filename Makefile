# Standard C stuff
CC := gcc
CFLAGS := -g -Wall
LDFLAGS :=
LDLIBS :=

# Create temporary dependency files
# as a side-effect of compilation
DEPFLAGS = -MT $@ -MMD -MP -MF $*.Td

# Rename the generated temporary dependency file to
# the real dependency file. We do this in a separate
# step so that failures during the compilation won’t
# leave a corrupted dependency file.
POSTCOMPILE = mv -f $*.Td $*.d

COMPILE = $(CC) $(DEPFLAGS) $(CFLAGS) $(CFLAGS_TGT) -c -o $@ $<
LINK = $(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

%.o: %.c
%.o: %.c %.d
	$(COMPILE)
	$(POSTCOMPILE)

# Create a pattern rule with an empty recipe,
# so that make won’t fail if the dependency
# file doesn’t exist.
%.d: ;

# Do not delete .d files automatically
.PRECIOUS: %.d

# Create object and dependency files
# from single source file
#   - $(1): <program_name>
#   - $(2): <CFLAGS_TGT>
define c-prog-compile
C_PROG_DIR := $(PLUGIN_DIR)/$(1)
SOURCES := $$(wildcard $$(C_PROG_DIR)/*.c)
OBJECTS := $$(SOURCES:%.c=%.o)
DEPS := $$(SOURCES:%.c=%.d)

$$(C_PROG_DIR)/%.o: CFLAGS_TGT := -I$$(C_PROG_DIR) $(2)

-include $$(DEPS)
CLEAN += $$(OBJECTS) $$(DEPS)
endef

# Compile and link in one step
#   - $(1): <program_name>
#   - $(2): <CFLAGS_TGT>
#   - $(3): <LDFLAGS>
#   - $(4): <LDLIBS>
define c-prog-complink
$$(eval $$(call c-prog-compile,$(1),$(2)))

PROGRAM := $$(C_PROG_DIR)/$(1)
PROGRAM_OUT := $(SM_PLUGINS_DIR)/$$(C_PROG_DIR)

$$(PROGRAM): LDFLAGS := $(3)
$$(PROGRAM): LDLIBS := $(4)
$$(PROGRAM): $$(OBJECTS)
	$$(LINK)

$$(PROGRAM_OUT): $$(PROGRAM)
	$$(INSTALL_BIN)

FILES_OUT += $$(PROGRAM_OUT)
CLEAN += $$(PROGRAM)
endef

# Used to substitue spaces
NOOP :=
SPACE := $(NOOP) $(NOOP)

SYMLINK := ln -s
INSTALL_BIN = install -m 0755 -D $< $@
INSTALL_DATA = install -m 0644 -D $< $@

# Returns '#!' if file is executable
#   - $(1): <path_to_file>
is-executable = $(findstring \#!,$(shell head -n 1 $(1)))

install-recipe = $(if $(call is-executable,$<),$(INSTALL_BIN),$(INSTALL_DATA))

# Construct decorated rule from executable file
# (has the form:  @<target_1>@...@<target_n>-><prerequisite>)
# e.g.: @SR.create@SR.destroy@SR.attach->volume/org.xen.xapi.storage.gfs2/sr.py
#
# $(REGEX) matches:
# - exactly 4 spaces
# - 'if' or 'elif' (capturing group 1)
# - 1 or more spaces
# - 'base'
# - 0 or more spaces
# - '=='
# - 0 or more spaces
# - single or double quotes
# - 0 or more characters (capturing group 2)
# - single or double quotes
# - ':'
# On match, sed returns: @<capturing group 2>
# Finally, all matches in the file are concatenated
# and '-><path_to_file>' is appended.
#   - $(1): <path_to_file>
REGEX := "s/ {4}(if|elif) +base *== *['\"](.*)['\"]:/@\2/p"
construct-decorated-rule = $(shell sed -nr $(REGEX) $(1) | tr -d '\n')->$(1)

# Returns the 1st path item name
# (dir_name if #items > 1; file_name if #items == 1)
#   - $(1): <path>
get-1st-path-item = $(firstword $(subst /,$(SPACE),$(1)))

# Splits the rule into 2 parts: targets and prerequisite
#   - $(1): <decorated_rule>
#
# N.B.: intermediate step
# - 1st word to be passed to 'create-targets'
# - 2nd word to be appended to '$(SM_PLUGINS_DIR)/' to
#   create the prerequisite
split-decorated-rule = $(subst ->,$(SPACE),$(1))

# Creates the rule's targets
#   - $(1): <sm_plugin_type>/<sm_plugin>
#   - $(2): $(word 1,$(call split-decorated-rule,<decorated_rule>))
create-targets = \
    $(addprefix $(SM_PLUGINS_DIR)/$(1)/,$(subst @,$(SPACE),$(2)))

# Returns the target the symlink will point to.
# The path is relative to the directory of the symlink
#   - $(1): <sm_plugin_type>/<sm_plugin>
#   - $(2): <prerequisite_out_path>
get-symlink-target = \
    $(if $(findstring $(1),$(2)),\
        $(subst $(SM_PLUGINS_DIR)/$(1)/,,$(2)),\
        $(subst $(SM_PLUGINS_DIR)/$(call get-1st-path-item,$(1)),..,$(2))\
    )

# Creates the file's install rule and appends a
# decorated symlink rule to the SM plugin's
# symlink rules (if file is executable)
#   - $(1): <sm_plugin_type>/<sm_plugin>
#   - $(2): <path_to_file>
define create-py-rule-from-file
ifneq (,$$(call is-executable,$(2)))
TMP := $$(call construct-decorated-rule,$(2))
PY_RULES += $$(if $$(findstring @,$$(TMP)),$$(TMP))
endif

$(SM_PLUGINS_DIR)/$(2): $(2)
	$$(install-recipe)

FILES_OUT += $(SM_PLUGINS_DIR)/$(2)
endef

# Generate 1 SM plugin rule
#   - $(1): <sm_plugin_type>/<sm_plugin>
#   - $(2): <decorated_rule>
define expand-decorated-py-rule
TMP := $$(call split-decorated-rule,$(2))
TARGETS := $$(call create-targets,$(1),$$(word 1,$$(TMP)))
PREREQ := $(SM_PLUGINS_DIR)/$$(word 2,$$(TMP))

$$(TARGETS): $$(PREREQ)
	$(SYMLINK) $$(call get-symlink-target,$(1),$$<) $$@

SYMLINKS_OUT += $$(TARGETS)
endef

# Makes all rules of an SM Plugin
#   - $(1): <sm_plugin_type>/<sm_plugin>
define make-sm_plugin-rules
PLUGIN_FILES := $(shell find $(1)/* -type f)
PYTHON_FILES_IN := $$(filter %.py,$$(PLUGIN_FILES))
PY_RULES :=

$$(foreach file,$$(PYTHON_FILES_IN), \
    $$(eval $$(call create-py-rule-from-file,$(1),$$(file))) \
)

PLUGIN_DIR = $(1)
-include $(1)/Rules.mk

$$(foreach rule,$$(PY_RULES), \
    $$(eval $$(call expand-decorated-py-rule,$(1),$$(rule))) \
)
endef

include Rules.mk
