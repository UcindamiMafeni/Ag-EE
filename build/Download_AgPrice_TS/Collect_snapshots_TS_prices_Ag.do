global path_in= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Temp"
global path_in_2= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Temp\Multimonth"
global path_in_3= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Temp\Files_2007_08"
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
destring yr, replace force
drop if missing(yr)

cd "$path_in_2"
local files: dir . files "*.dta"

qui foreach f in `files' {
	append using "`f'"
}

cd "$path_in_3"
local files: dir . files "*.dta"

qui foreach f in `files' {
	append using "`f'"
}

save "$path_out/Snapshots_collected_3.dta", replace
