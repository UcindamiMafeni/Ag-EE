global path_in= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Temp"
global path_out= "C:\Users\clohani\Dropbox\California_Crop_Snapshots"

clear
cd "$path_in"
local files: dir . files "*.dta"
set obs 1
gen to_del=.
qui foreach f in `files' {
	append using "`f'"
}
drop if _n==1
drop to_del

save "$path_out/Snapshots_collected.dta", replace
