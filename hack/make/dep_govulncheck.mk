$(call _assert_var,MAKEDIR)
$(call _conditional_include,$(MAKEDIR)/base.mk)
$(call _assert_var,CACHE_VERSIONS)
$(call _assert_var,CACHE_BIN)

GOVULNCHECK := $(CACHE_VERSIONS)/govulncheck/latest
.PHONY: $(GOVULNCHECK) # Ignored cached version, always download the latest.
$(GOVULNCHECK):
	rm -f $(CACHE_BIN)/govulncheck
	mkdir -p $(CACHE_BIN)
	echo Downloading golang.org/x/vuln/cmd/govulncheck@latest
	env GOBIN=$(CACHE_BIN) go install golang.org/x/vuln/cmd/govulncheck@latest
	rm -rf $(dir $(GOVULNCHECK))
	mkdir -p $(dir $(GOVULNCHECK))
	touch $(GOVULNCHECK)
