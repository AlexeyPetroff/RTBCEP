#!/usr/bin/python

import json
import sys
import uuid
from time import sleep, time

import random
from kafka import KafkaProducer

countries = ['Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 'Anguilla', 'Antigua & Barbuda', 'Argentina',
			 'Armenia', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus',
			 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia', 'Bosnia & Herzegovina', 'Botswana', 'Brazil',
			 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Myanmar/Burma', 'Burundi', 'Cambodia', 'Cameroon',
			 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China',
			 'Colombia', 'Comoros', 'Congo', 'Costa Rica', 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic',
			 'Democratic Republic of the Congo', 'Denmark', 'Djibouti', 'Dominican Republic', 'Dominica',
			 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Fiji',
			 'Finland', 'France', 'French Guiana', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Great Britain',
			 'Greece', 'Grenada', 'Guadeloupe', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Honduras',
			 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Israel and the Occupied Territories',
			 'Italy', 'Ivory Coast (Cote d\'Ivoire)', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kosovo',
			 'Kuwait', 'Kyrgyz Republic (Kyrgyzstan)', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya',
			 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Republic of Macedonia', 'Madagascar', 'Malawi', 'Malaysia',
			 'Maldives', 'Mali', 'Malta', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico',
			 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique',
			 'Namibia', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria',
			 'Korea, Democratic Republic of (North Korea)', 'Norway', 'Oman', 'Pacific Islands', 'Pakistan', 'Panama',
			 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar',
			 'Reunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Kitts and Nevis', 'Saint Lucia',
			 'Saint Vincent\'s & Grenadines', 'Samoa', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia',
			 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovak Republic (Slovakia)', 'Slovenia', 'Solomon Islands',
			 'Somalia', 'South Africa', 'Korea, Republic of (South Korea)', 'South Sudan', 'Spain', 'Sri Lanka',
			 'Sudan', 'Suriname', 'Swaziland', 'Sweden', 'Switzerland', 'Syria', 'Tajikistan', 'Tanzania', 'Thailand',
			 'Timor Leste', 'Togo', 'Trinidad & Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks & Caicos Islands',
			 'Uganda', 'Ukraine', 'United Arab Emirates', 'United States of America (USA)', 'Uruguay', 'Uzbekistan',
			 'Venezuela', 'Vietnam', 'Virgin Islands (UK)', 'Virgin Islands (US)', 'Yemen', 'Zambia', 'Zimbabwe']

models = ['SM-G800F', 'M8', 'Android 2.0', 'PLK-L01', 'sm-g930f', 'SM-G900FQ', 'GT-I9505', 'SM-G900F', 'ALE-L21',
		  'SM-G930T', 'SM-G900I', 'SM-G928V', 'Desire 650', 'SM-J320F', 'SM-G925F', 'SM-G955F', 'MS345', 'C6603',
		  'SM-N910F', 'SM-A500FU', 'Blade A462', 'XT1032', 'NX531J', 'SM-G920I', 'CPH1607', 'Android 2.0 Tablet',
		  'SM-G950F', 'SM-G850F', 'XT1585', 'SM-G935F', '55 Platinum', 'Z010D', 'SM-G530R7', 'EVA-AL10', 'ROBBY',
		  'L52', 'H815', 'Z959', 'F670S', 'A0001', 'SM-J327T1', 'SM-A520F', 'LGLS775', 'SM-G920F', 'Pixel', 'SM-A300FU',
		  'A5000', 'EVA-L09', 'Hudl 2', 'Z981', 'Nexus 6P', 'SM-T820', 'F8331', 'SM-N920I', 'D802T', 'F3111', 'A2017G',
		  'A3-A40', '2PS64', 'SM-N9005', 'Puls', 'SM-J710FQ', 'SCL-L01', 'VTR-L09', 'BLADE A110', 'SM-N920C', 'SM-T280',
		  'A3003', 'SM-N910R4', 'Blade L3', '0PKV1', 'VNS-L31', 'SM-N915T', '101e Neon', 'SM-J510FN', 'SM-T550',
		  'SM-N900A', 'SM-G920W8', 'SGH-I337', 'SM-G903F', 'H830', 'SM-G928T', 'XT1505', 'SM-J510MN', 'N9519',
		  'SM-J500F', 'US990', 'LGMS550', 'P9000', 'TAB1043', 'D626X', 'Redmi 3S', 'SM-S903VL', '8070', 'SM-G935P',
		  'Xperia D6653', 'SM-S327VL', 'SM-G901F', 'SM-G930F', 'D5503', 'MHA-L09', 'Redmi 3', 'A1601', 'X007D',
		  'SM-T560NU', 'Redmi Note 4X', 'sm-j510fn', 'Q351', 'SM-T530', 'SM-G389F', 'PRA-LX1', 'X00AD', 'SM-G900T',
		  'H915', 'Moto G (4)', 'Z012D', 'SM-J327P', 'VFD 700', 'Moto G Play', 'K520', 'SM-G531F', 'X220', 'SM-G532M',
		  'SM-G930A', 'NEM-L21', 'SM-G928P', 'SM-G928C', 'SM-N7505', 'D6603', 'X600 LTE', 'SM-J500H', 'was-lx1a',
		  'SM-G900FD', 'SM-N910G', 'MS210', 'Desire 820', 'SM-J500FN', 'SM-S120VL', 'LGL82VL', 'D5803', 'Rainbow',
		  'XT1033', 'I9506', 'A9', 'K33 a48', 'Redmi Note 3', 'One M8s', 'SM-T535', 'LGLS755', 'HTCD160LVWPP', 'W4',
		  'MotoG3', 'SM-G930P', 'XT1254', 'NXT-L09', 'G3312', 'Z988', 'Nexus 5X', 'ONE A2003', 'LGLS676', 'CUN-L21',
		  'Blade Q Lux', 'SM-J100VPP', 'generic', 'SM-A500F', 'SM-G920A', 'SM-G928I', 'FRD-L09', 'R7sf', 'SM-G570Y',
		  '9001X', 'SM-J320AZ', 'F3115', 'SM-G920V', 'Lenny 2', '5054W', 'SM-J120FN', 'SM-T377W', 'Pixel XL',
		  'SM-G900W8', 'E6653', 'SM-T555', 'P00C', 'SM-N900T', 'T03', 'G7-L01', 'E5823', 'JERRY', 'SM-J320A', 'NXT-L29',
		  'Z956', 'P023', 'B3-A30', 'E6883', 'sm-a510f', '3622A', 'SM-G955U', 'RAINBOW JAM 4G', 'E2303', 'S6', 'Mi 5',
		  'B2016', 'LENNY3', '5056N', 'B1-780', 'K420', 'SM-G930T1', 'Blade A452', 'A7010a48', 'D6503', '101c Copper',
		  'STV100-4', 'CRR-L09', 'M9', 'GT-I9295', 'XT1562', 'LGL62VL', 'sm-g935f', 'Nexus 5', 'One Touch Idol 4 6055K',
		  'KIW-L21', 'Rome_X', 'S60', 'LG-K220', 'SM-T705Y', 'M7', 'SM-T350', 'K330', 'E1003', '10', 'e6553',
		  'SM-A310F', 'One M7', 'A01W', 'SPACE10_PLUS_3G', 'VFD 500', 'SM-N920A', '5022E', 'SM-T805', 'SM-J700P',
		  'SGH-I537', 'SCH i545', 'A57', 'Ridge Fab 4G', 'GRA-L09', 'K017', 'SM-T810', '2PS5200', 'SM-J321AZ',
		  'GT-I9515', 'SM-G900V', 'LIFETAB_P831X.2', 'Desire 530', 'Z832', 'Sunny', 'A2016a40 B', 'VNS-L21',
		  '70c Xenon', 'SM-J727V', 'XT1092', 'NXA8QC116', 'sm-g900v', 'K10000', 'BLADE L110', 'T00J', 'SM-N915FY',
		  'H990', 'sm-g925f', 'SCL-L21', 'sm-g900f', '4060W', 'sm-g950f', 'F3311', 'LGLS450', 'SM-N9208', 'A6020a40',
		  'MI 4W', 'GT-I9500', 'sm-g903f', 'K450', 'd6503', 'S40', 'SM-T819Y', 'SM-J120AZ', 'H1611', 'VS', 'NEM-L51',
		  'PLK-AL10', 'H955', 'Picasso', 'Rainbow Jam', 'sm-g920f', 'H540', 'SGH-I337M', 'Tommy', 'H850', 'X9009',
		  'nx541j', '5045D', 'SM-J200F', 'U FEEL LITE', 'E6810', 'X5004', 'E6553', 'SM-N900P', 'SM-S320VL', 'T84',
		  'U FEEL', 'LGL52VL', 'VNS-L22', 'SM-N9007', 'BQS-5065', 'SM-T537V', 'M2 Note', 'BLN-L21', 'H860',
		  'Aquaris X5', 'SM-J500M', '2PST2', 'RIO-L02', 'K580', '0P6B', 'Ridge 4G', 'H60-L04', 'CUN-L01', 'TB2-X30F',
		  'LS675', 'sm-n9005', 'VKY-L09', 'SM-A300Y', 'MHA-L29', 'SM-J120A', 'Pulp Fab 4G', 'moto g (4)', 'SM-T330NU',
		  'SM-T800', 'SM-T355Y', 'E6560', 'SM-G906L', 'K500n', 'SM-G930V', 'G3221', 'SM-J700F', 'ASUS_Z00AD', 'F5321',
		  'BTV-DL09', 'D2303', 'G3311', 'Thor', 'SCH i545PP', 'A10-70F', 'SM-G935V', 'SM-G360F', 'SM-A520W', 'Note S',
		  'CHC-U01', 'MT7-L09', 'SM-G550T', 'GT-I9507', 'sm-g800f', 'SM-G530FZ', 'Redmi 4X', 'B1-770', 'ale-l21',
		  'TAG-L22', 'Blade V6', 'T08', 'Pulp 4G', 'Nexus 7', 'SM-T700', 'm3s', 'vns-l31', 'D530u', 'Swift 2 Plus',
		  'Max', '2PS6200', 'MP260', 'SM-G360G', 'SM-G530AZ', 'Z00UD', 'nmo-l31', 'sm-j320f', 'XT1068', 'E2353',
		  'ONE A2001', 'One A9s', 'PGN611', 'VFD 600', 'Z828', 'SM-A710F', 'SM-E700H', 'XT1650', 'SM-G891A',
		  'Shield Tablet K1', 'SM-N910V', 'G3121', 'F8131', 'LG-X210', 'D620', 'Blade L5', 'SCL-U31', 'a3003',
		  'Nexus 10', 'Highway Pure', 'K10a40', 'SM-G935A', 'Xperia Z1', 'GM 5 Plus', 'LG-US996', '5080X', 'Y53 1606',
		  'LUA-U22', 'K007', 'SM-S727P', 'E1005', 'LG-M250', 'XT1635-02', 'Blade V7', 'SM-P550', 'G800Y',
		  'BLADE V7 LITE', 'K120', 'SM-G9350', 'SM-S907VL', 'Z833', '6039y', 'C6602', 'SM-P605', 'C6883', 'E5603',
		  'SM-P600', 'Apollo Lite', 'F3211', 'K350', 'A51f', 'D722', 'TP450', 'LGLS992', 'SM-G610Y', 'Moto G (5) Plus',
		  'VS425', 'D855', '2PST1', 'K01B', 'T6S', 'L-ement Tab 744P', 'T610', 'PB1-750M', 'SM-G930W8', 'SM-J700T',
		  'h815', 'Z00VD', 'sm-n910f', 'P7000', 'F20', 'LYO-L21 Honor 5A', 'XT1526', 'XT1028', 'R2507', 'SM-J320VPP',
		  'U11', '8050D', 'K6000 Pro', '8079', 'One E9+', 'pra-lx1', 'TB3-710F', 'LGMS428', 'H525n', 'Nexus 4',
		  'DMC-CM1', 'KII-L22', 'SM-S920L', 'C6903', 'SM-T237P', 'S8-50L', 'SM-G900T3', 'Android 5.0', 'SM-G570F',
		  'XT1021', 'MX6', 'SM-G870A', 'B2017G', 'thor', 'Excite Prime', 'MI-3W', '1201', 'sm-g531f', 'K373', 'H870DS',
		  'Redmi Note 4', 'SM-A300F', 'SM-N910P', 'Nexus 6', 'M20 Pro', 'SM-G610M', 'MI 5s', 'SM-P350', 'VFD 1100',
		  'F820L', 'H870', 'Fire2 LTE', 'XT1063', 'Galaxy P3110', 'X537', 'Z00ED', 'MX5', 'STH100-2', 'SM-A9000',
		  'SCL-TL00H', 'G3116', 'Z963VL', 'VF-795', 'SM-T533', 'M3s', 'One ME', 'LS770', 'XT1575', 'SM-N910A', 'A50TI',
		  '5017X', 'PSP3530DUO', 'Blade V0720', 'S8-50F', 'SM-G935W8', 'T6C', 'Desire 816', 'D5883', 'X15', 'Z799VL',
		  'Desire', 'sunny', 'H650', 'XT1039', 'U Play', 'PRA-LA1', 'XM100', 'xplorer V80', 'R7g', 'N9132', 'SM-A700YD',
		  'SM-A300G', 'SM-N910C', 'Spira B2 5 Smartphone', 'Aqua Life III', 'Desire 728', 'SM-N920T', 'Z958', 'Fever',
		  '5065N', 'H345', 'P8000', 'SM-J320G', 'DM-01G', 'ATH-UL01', 'F5', 'TAB 2 A8-50F', 'YT3-X50F', 'Q372',
		  'L-EMENT551', 'Xperia SGP612', 'C6743', 'SM-G531H', 'Z812', 'X815', 'redmi note 4', 'f3111', 'LYO-L02',
		  'PB2-650M', 'E485', 'E6853', 'A2010-a', 'VS980', 'K50a40', 'Mi Max', 'SM-E500H', 'Blade V580', 'SUPER',
		  'XT1056', 'K540', 'SM-J320V', 'X5Pro', 'SM-G360T', 'H818', 'Smart Ultra 6', '5010D', 'H420', 'SM-G906S',
		  'SM-G920T1', 'STARXTREM 5', 'BV6000', 'X11', 'z00ad', 's1034x', 'sm-a310f', 'SM-G920P', 'K013', 'LS991',
		  'SM-N920P', 'SM-J320H', 'SM-G550T2', 'H918', 'H831', 'K53a48', 'LIFETAB S1036X', 'SPH-L720', 'Blade L6',
		  'HT17Pro', 'XT1096', 'T04', 'sm-a520f', 'MS631', '0PJA10', 'Blade L5 Plus', 'CUN-U29', 'HTCD100LVWPP',
		  'Life One x', 'CAN-L11', 'Freddy', 'e6653', 'LIFETAB P891X', 'VS987', 'Fire Plus LTE', 'SM-J700M', '0P6B6',
		  'SGP321', '0PGQ1', 'P01Z', 'VF1497', 'F5121', 'SM-G361H', 'LS665', 'f8331', 'O1', 'F1f', 'eva-l09', 'SM-G925',
		  'M2', 'SM-J320R4', 'SM-G890A', 'R1 HD', '?', 'SM-G900P', 'NOTOS Plus 3G', 'TP601A', 'E5633', 'SM-A500H',
		  'sm-j500fn', 'SM-N920V', 'SM-S727VL', 'SM-T337T', 'F3215', 'Xperia SGP611', 'P1ma40', 'Z016D', 'SM-N900V',
		  'SM-J510F', 'SM-N916L', 'D2502', 'a2017g', 'Dslide714', 'sm-a300fu', 'PRO 6', 'gt-i9505', 'D852', 'MS330',
		  'Trek HD', '6039Y', 'Z831', 'RIO-L01', 'Dinosaur', 'M12', 'Iris X5', 'SM-J111F', 'A3010', 'S1La40',
		  'SM-G530H', 'K130', 'D100', 'M9 Plus', 'VS988', 'SM-J320P', 'VS501', 'SM-A8000', 'Desire 628', 'T807V',
		  'XT1706', 'H60', 'XT1609', 'SM-A500G', 'Rainbow Lite 4G', 'x551', 't07 liquid zest 4g', 'Boom J8', 'vtr-l29',
		  'TP260', 'D728x', 'BBB100-2', 'A10-70L', 'sm-g901f', 'A1010a20', 'TA-1053', 'Mi Note 2', '50 Platinum 4G',
		  'XT1023', 'sm-j710f', 'XT1072', 'P7-L10', 'X9079', 'RAINBOW UP', 'M10h', 'SM-T377V', 'SM-T585', 'H812',
		  'Star Trail 7', 'MFC511FR', 'F8332', 'XT1045', 'SM-T705', 'VFD 200', '50c Platinum', 'X510', 'LG-M150',
		  'MID4X10', '55 Helium', 'Pixel C', 'TB3-850F', 'SM-G903W', 'SM-G925W8', 'Pixel V2+', 'StarXtrem 4', 'A7000',
		  'X557 Lite', 'SM-G750A', 'P6000', 'LGUS992', 'c6903', 'X12', 'K50-t3s', '6045O', 'LGUS375', 'SM-N900',
		  'A7-10', '6045Y', 'ME581CL', 'SM-A500W', 'A1000', 'lyo-l21 honor 5a', 'SM-J320W8', 'Phantom5', 'H340n',
		  'P022', 'C50_Trendy', 'Discovery 111C', 'SM-G530T1', 'LS740', 'SM-J510GN', 'EVA-L19', 'L678', 'SM-J106F',
		  'H872', 'Z963U', '5051X', 'STV100-2', 'SM-P555', 'M5 Note', 'm9', '5070D', 'K100', '5051D', 'VS425PP',
		  'SM-N910U', 'E6683', 'XT1528', 'VS880PP', 'gt-i9515', 'L-EMENT503', 'FDR-A01L', 'SM-T337A', '2015A', 'K500',
		  'Aquaris M5', 'Rise 10', 'FRD-L19 Honor 8 Premium', 'Z017DA', 'plk-l01', 'RAIN A3000', 'Elite 5', 'SM-A800F',
		  '7C', 'SM-N910T', 'Redmi Note 2', 'VIE-L09', 'SM-N915V', 'U007', 'Desire 626 Dual Sim', 'XT1034', 'R7Plusf',
		  'G3123', 'XT1093', 'K428', 'Desire 10 Lifestyle', 'H901', 'B1-850', 'Z010DD', 'LUA-L02', 'sm-g955f',
		  'NX16A8116K', 'C3 3G', 'K425', 'SM-T377A', 'CAN-L01', 'SM-G906K', 'LS993', 'Y538', 'Android 6.0', 'Z017D',
		  'P01Y', 'E561_EU', 'sm-g928f', '1050F', 'robby', 'a5000', 'VFD 1400', 'SM-J710GN', 'LGL44VL', 'BLADE A512',
		  'E6833', 'X557', 'M260', 'ZUK Z2121', 'K550', 'T02', 'SM-G925V', 'SCH-R970', 'Redmi 4A', 'Xperia SGP621',
		  'A7020a48', '4034D', 'Highway Star', 'MyTablet 10 Inch', 'Venue 10 5050', 'L-EMENT 505', 'E8', 'ZUK Z1',
		  '7043K', 'LG-AS330', 'sm-a500fu', 'fp2', '6055B', 'SM-J500G', 'C6902', 'Aquaris E5', 'VS600PP', 'M2-801W',
		  'XT1563', 'X008DD', 'S7', '45b Neon', 'Elite10QS', 'Z716BL', 'NMO-L31', 'SM-T335', 'NX511J', 'SM-J710MN',
		  'T06', 'Desire 728G', 'BLADE A112', 'X9', 'SM-N920K', 'f8131', 'A466BG', 'H631', 'SM-G927', 'H440n',
		  '50 Cobalt', 'SM-G925A', 'Flare J1', 'Studio C Super Camera', 'SM-J700H', 'Aquaris M5.5', 's30', 'M5',
		  'LUA-U03', 'Z820', 'London', 'C20', 'Touch', 'B3-A20', '895n', 'Android 5.0 Tablet', 'Spira B1 10.1', 'S90-A',
		  'MI 5s Plus', 'redmi 3s', 'X5max', 'One Max', 'SM-G928F', 'BV6000S', 'SM-G600FY', 'Vivo 5R', '0PFJ50',
		  'SCC-U21', 'F8132', '2PQ910', '5025D', 'Y100 Pro', 'P2a42', 'Power', 'Z818L', '5010X', '9001D', 'LG-LS997',
		  'Blade A510', 'f5321', 'd855', '830F', 'P01T_T', 'C5503', 'ALE-L02', 'X5S', 'Pixel V2', 'Desire 626s',
		  '5054N', 'TAG-L01', 'm2', 'YT-X703F', 'K88', 'SM-G360V', 'Z90a40', 'A95X', 'SM-G530W', 'GT-I9195', 'C15100m',
		  'H320', 'MP71 OCTA', 'BBA100-2', 'Diamond S', 'blade v6', 'X008D', 'Spark X', 'P008', 'NX513J', 'Z010DA',
		  'U7 PRO', 'SGP512', 'L53', '50e Neon', 'XT1022', 'H634', 'CAM-L21', 'KFGIWI', 'Z899VL', 'TB3-730X', 'A570BL',
		  'FDR-A01w', 'A37f', 'G3212', 'D415', 'XT1058', 'G8231', 'HS-U602', 'vtr-l09', 'F3213', 'YT3-850F', 'N9518',
		  'redmi note 3', 'V410', '5054D', 'C6503', '101b Oxygen', 'Desire 826', 'ze551ml', 'HTCD100LVW', 'Next',
		  'SM-G6000', 'H710VL', 'Energy X Plus', 'SM-T355', 'NXT-AL10', 'E2363', 'Eluga I3', 'E5303', 'KFTBWI',
		  'SM-N910K', 'HTC6545LVW', 'SGP511', 'E653', 'E5653', 'D2302', 'SM-G900K', 'SM-N910S', 'P1050X', 'YB1-X90F',
		  'Aquaris A4.5', 'X6', 'R715Q', 'Pulp', 'E2333', 'Lux 10']

campaign_ids = []
ad_ids = {}
pub_ids = []
dev_ids = []


def fill_random_campaign_ids():
	#for i in range(2):
	campaign_ids.append(str(uuid.uuid4()))


def fill_random_ad_ids():
	for campaign_id in campaign_ids:
		for _ in range(3):
			ad_ids.setdefault(campaign_id, []).append(str(uuid.uuid4()))


def fill_random_pub_ids():
	for _ in range(1000):
		pub_ids.append(str(uuid.uuid4()))


def fill_random_dev_ids():
	for _ in range(10000):
		dev_ids.append(str(uuid.uuid4()))


def random_country(dev_id):
	if random.random() < 0.988:
		h = hash(dev_id)
		random.seed(h)
	country = random.choice(countries)
	random.seed()
	return country


def random_model():
	return random.choice(models)


def random_age():
	return random.randint(10, 50)


def random_event():
	events = []
	camp_id = random.choice(campaign_ids)
	ad_id = random.choice(ad_ids[camp_id])
	timestamp = time()
	dev_id = random.choice(dev_ids)
	country = random_country(dev_id)
	os = random.choice(['Android', 'OS'])

	model = random.choice(models)
	age = random_age()
	gender = random.choice(['M', 'F'])
	pub_id = random.choice(pub_ids)
	impression = {'capaign_id': camp_id, 'ad_id': ad_id, 'timestamp': timestamp, 'country': country, 'os': os,
				  'dev_id': dev_id, 'model': model, 'age': age, 'gender': gender, 'pub_id': pub_id,
				  'type': 'impression'}
	events.append(impression)
	if random.random() < 0.01:
		if random.random() < 0.98:
			sleep(1)
		click = dict(impression, **{'timestamp': time(), 'type': 'click'})
		events.append(click)
		if random.random() < 0.25:
			conversion = dict(impression, **{'timestamp': time(), 'type': 'conversion'})
			events.append(conversion)
	return events


server = "localhost:9092"


def main():
	# the topic
	topic = sys.argv[1]

	# create a Kafka producer
	producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=server)
	print("*** Stream on " + server + ", topic : " + topic + "***")
	fill_random_campaign_ids()
	fill_random_ad_ids()
	fill_random_pub_ids()
	fill_random_dev_ids()
	try:
		while True:
			for _ in range(1, 2000):
				# get random events
				events = random_event()
				for e in events:
					producer.send(topic, e, key=e['ad_id'])
					print("Sending event: %s" % (json.dumps(e).encode('utf-8')))
			sleep(7)

	except KeyboardInterrupt:
		pass

	print("\nSTOPPED!")
	producer.flush()


if __name__ == "__main__":
	main()
