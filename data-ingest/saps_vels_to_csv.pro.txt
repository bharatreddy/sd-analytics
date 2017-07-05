pro saps_vels_to_csv


common radarinfo
common rad_data_blk


date_rng = [ 20160101, 20161231 ]
timeRange=[0000,2400]

allRadIdsList = network[*].id
allRadCodeList = network[*].code[0]


del_jul_nday=24.d*60.d/1440.d

sfjul, date_rng, [0000, 2400], sjul_nday, fjul_nday

ndays_search=((fjul_nday-sjul_nday)/del_jul_nday)+1 ;; Num of 2-min times to be searched..


sfjul, date_rng, timeRange, sjul_day, fjul_day



print, "ndays_search", ndays_search
for srchDay=0.d,double(ndays_search) do begin

	juls_day=sjul_day+srchDay*del_jul_nday
	sfjul,dateDay,timeDay,juls_day,/jul_to_date
	;; loop through all the radars
	for rcInd = 0, n_elements(allRadCodeList)-1 do begin
		print, allRadCodeList[rcInd]

		;rad_fit_read, dateDay, allRadCodeList[rcInd]
		;data_index = rad_fit_get_data_index()
		;print, Tag_Names(*rad_fit_data[data_index])
		;print, Tag_Names(*rad_fit_info[data_index])

	endfor

endfor


end