# Covenant_Violation_Finder
 Collect covenant violation data for the 10-Qs and 10-Ks from SEC EDGAR. Conduct textual analysis to identify filings with possible covenant violation events. Identify the occurrence of a covenant violation, collect a dummy variable that indicates a company’s debt covenant violations. Used the covenant violation data to generate descriptive statistics and correlations to examine the determinants of covenant violations.

# Algorithm used
 Algo: Find Word= “covenant”, Search for words that include  “waiv,” “viol,” “in default,” “modif,” & “not in compliance.” in 3 lines above and below the word “covenant”. Gives violations in their test dataset