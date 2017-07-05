pro saps_vels_to_csv


common radarinfo
common rad_data_blk


date_rng = [ 20160101, 20160101 ]
timeRange=[0000,2400]

allRadIdsList = network[*].id
allRadCodeList = network[*].code[0]
coords = "magn"

del_jul_nday=24.d*60.d/1440.d

sfjul, date_rng, [0000, 2400], sjul_nday, fjul_nday

ndays_search=((fjul_nday-sjul_nday)/del_jul_nday)+1 ;; Num of 2-min times to be searched..


sfjul, date_rng, timeRange, sjul_day, fjul_day


baseDir = '/home/bharatr/Docs/data/velFiles-2016/'

for srchDay=0.d,double(ndays_search) do begin

	juls_day=sjul_day+srchDay*del_jul_nday
	sfjul,dateDay,timeDay,juls_day,/jul_to_date

	if srchDay eq 0.d then begin
		fname_saps_vel = baseDir + 'vels-' + strtrim( string(dateDay), 2 ) + '.txt'
		print, 'OPENING FIRST EVENT FILE-->', fname_saps_vel
		openw,1,fname_saps_vel
		workingSapsFileDate = dateDay
	endif else begin
		if workingSapsFileDate ne dateDay then begin
			print, 'CLOSED FILE-->', fname_saps_vel
			close,1
			fname_saps_vel = baseDir + 'vels-' + strtrim( string(dateDay), 2 ) + '.txt'
			print, 'OPENING NEW FILE-->', fname_saps_vel
			openw,1,fname_saps_vel
			workingSapsFileDate = dateDay
		endif
	endelse


	
	;; loop through all the radars
	for rcInd = 0, n_elements(allRadCodeList)-1 do begin
		

		rad_fit_read, dateDay, allRadCodeList[rcInd]
		data_index = rad_fit_get_data_index()
		if data_index eq -1 then begin
			print, "nodata------------------"
			continue
		endif
		allJulData = (*rad_fit_data[data_index]).juls
		for jcnt=0, double( n_elements( uniq(allJulData) ) ) - 1  do begin
			; convert to date and time
			juls_curr = allJulData[jcnt]
			caldat, juls_curr, mm, dd, year
			yrsec = (juls_curr-julday(1,1,year,0,0,0))*86400.d
			sfjul,datesel,timesel,juls_curr,/jul_to_date

			scan_number = rad_fit_find_scan(juls_curr)
			velArr = rad_fit_get_scan(scan_number, scan_startjul=juls_curr)
			powArr = rad_fit_get_scan(scan_number, scan_startjul=juls_curr, param="power")
			widArr = rad_fit_get_scan(scan_number, scan_startjul=juls_curr, param="width")
			scan_beams = WHERE((*rad_fit_data[data_index]).beam_scan EQ scan_number and $
						(*rad_fit_data[data_index]).channel eq (*rad_fit_info[data_index]).channels[0], $
						no_scan_beams)

			; get mlat, mlon of fov
			rad_define_beams, (*rad_fit_info[data_index]).id, (*rad_fit_info[data_index]).nbeams, $
					(*rad_fit_info[data_index]).ngates, year, yrsec, coords=coords, $
					lagfr0=(*rad_fit_data[data_index]).lagfr[scan_beams[0]], $
					smsep0=(*rad_fit_data[data_index]).smsep[scan_beams[0]], $
					fov_loc_full=fov_loc_full, fov_loc_center=fov_loc_center


			

			;; get the data
			sz = size(velArr, /dim)
			radar_beams = sz[0]
			radar_gates = sz[1]


			print, "curr radar, day---->", allRadCodeList[rcInd], datesel,timesel

			; loop through and extract
			for b=0, radar_beams-1 do begin
				for r=0, radar_gates-1 do begin
					if velArr[b,r] lt 10000 then begin
						; magnetic coords
						currMLat = fov_loc_center[0,b,r]
						currMlon = fov_loc_center[1,b,r]
						currMLT = mlt(year, yrsec, fov_loc_center[1,b,r])
						; geo coords
						gcrd = CNVCOORDDAVIT(currMLat, currMlon, 300., /geo)
						;; gflag, freq, azim
						currGflag =  (*rad_fit_data[data_index]).gscatter[scan_beams[b],r]
						currFreq = (*rad_fit_data[data_index]).tfreq[scan_beams[b]]
						currbeamAzim = rt_get_azim(allRadCodeList[rcInd], b, datesel)
						currVelErr = (*rad_fit_data[data_index]).velocity_error[scan_beams[b],r]

						strTimesel = strtrim( string(timesel), 2 )

						if timesel lt 10 then begin
							strTimesel = strtrim( string(0), 2 ) + strtrim( string(0), 2 ) + strtrim( string(0), 2 ) + strtrim( string(timesel), 2 )
						endif
						if ( (timesel ge 10) and (timesel lt 100) ) then begin
							strTimesel = strtrim( string(0), 2 ) + strtrim( string(0), 2 ) + strtrim( string(timesel), 2 )
						endif

						if ( (timesel ge 100) and (timesel lt 1000) ) then begin
							strTimesel = strtrim( string(0), 2 ) + strtrim( string(timesel), 2 )
						endif


						printf, 1, datesel, strTimesel, b, r, currbeamAzim, velArr[b,r], currVelErr, powArr[b, r], widArr[b, r], $
								currMLat, currMlon, currMLT, allRadIdsList[rcInd], allRadCodeList[rcInd], $
								currGflag, currFreq, gcrd[0], gcrd[1], (*rad_fit_info[data_index]).nbeams, (*rad_fit_info[data_index]).ngates, $
		                                format = '(I8, A6, 2I4, f9.4, 4f11.4, 2f11.4, f9.4, I5, A5, I5, f11.4, 2f11.4, 2I5)'
		            endif

	                           
				endfor
			endfor

		endfor

		
		;print, Tag_Names(*rad_fit_data[data_index])
		;print, Tag_Names(*rad_fit_info[data_index])

	endfor

endfor

close,1

end