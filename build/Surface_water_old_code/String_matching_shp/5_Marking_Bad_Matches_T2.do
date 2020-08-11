// list of good matches for Tier_2

do 0_Set_Path.do

global path_in= "$path_master/Cut_1"
global path_out= "$path_master/Cut_1"

#delimit ;
local good_matches 31 75 126 134 151 163 178 179 184 191 236 239 281 
	349 363 374 475 376 377 378 406 423 442 444 455 468 469 471 472 473 474 475 476 477 478 484
	488 492 495 497 499 500 513 530 531 532 534 545 547 548 551 553 554 557
	559 560 561 566 567 568 573 578;
#delimit cr

forvalues i=291(1)321 {
	local good_matches `good_matches' `i'
}

forvalues i=334(1)348 {
	local good_matches `good_matches' `i'
}

forvalues i=514(1)527 {
	local good_matches `good_matches' `i'
}

forvalues i=535(1)542 {
	local good_matches `good_matches' `i'
}

use "$path_in/Tier_2.dta", clear
replace bad_match=1

foreach i of local good_matches {
	replace bad_match=0 in `i'
}

save "$path_out/Tier_2.dta", replace
