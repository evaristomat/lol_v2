# função que vai ler as apostas pendentes em lol_bets.db
# table bets coluna bet_status se for pending ou to_verify, então analisar, outro alem desses já tem resultado (won or lost)

# depois de termos todas as apostas pendentes vamos verificar UMA por UMA para ir atras do resultado
# Primeiro precisamos entender se é uma aposta de Totals ou de Player, vamos descobrir isso  pelo market_type se for player vai
# ter a palavra Player no valor por exemplo market_type = "Map 1 - Player Total Assists", ou "Map 2 - Player Total Kills" etc
#  se for total vamos ter "Map 1 - Totals" ou "Map 2 - Totals"

# para apostas de Totals, temos algumas selections como
# Under Total Kills, Over Total Kills, Under Total Barons, Over Total Barons
# Under Total Dragons, Over Total Dragons, Under Total Inhibitors, Over Total Inhibitors
# Under Game Duration, Over Game Duration, Under Total Towers, Over Total Towers

# para cada tipo desse precisamos criar uma logica para buscar no banco lol_history.db o resultado do evento e
# pegar corretamente o resultado da selection e comparar a selection da bet com o resultado real e definir se a bet foi
# won or lost

# Apos cada bets ter sido confirmada como won or lost, atualizar no banco de dados na linha da bet o seu resultado
# precisamos atualizar a linha acutal_win com o lucro da aposta, sendo lucro se vencedorar = odd - 1, se perdedora lucro = -1
# atualizar o result_verified para check, atualizar updated_at, atualizar o bet_status para won or lost se aposta com resultado
# se não encontrado resultado manter como pending ou to_verify (como estava)
# atualizar a linha acutal_value com a linha do resultado da seleciton por exemplo se a aposta foi under total dragons
# e a coluna line (coluna com o valor da linha da aposta) estiver 4.5 e o resultado do total_dragons da partida foi de 5,
# atualizamos actual_value para 5, e aqui nesse caso a aposta foi perdedora porque 5 é maior que 4.5 e a aposta era under 4.5
# usaremos essa mesma logica para todos os mercados de totals

# existe distincão de mapa, as vezes a aposta é map1 ou map 2 e precisa buscar o resultado do mapa exato apostado
# temos a coluna map_number e algumas apostas estão vazio por serem antigas e podemos usar o market_type para tirar de la se map 1 ou map 2

# ja para apostas de player precisamos buscar o resultado no csv data/database/database.csv
# em market_type de player temos 3 mercados diferentes
# Kills, Assists e Deaths, vamos encontrar Map 1 - Player Total Assists, Map 2 - Player Total Assists
# Map 1 - Player Total Kills, Map 2 - Player Total Kills
# Map 1 - Player Total Deaths, Map 2 - Player Total Deaths
# já em selection teremos Over ou Under e o nome do player por exemplo Over River, Under Elk
# Se Player Total Kills e Over River e line 3.5 significa que aposta é para o Jogador River tem mais de 3.5 kills no mapa
# ele ter matado pelo menos 4 jogadores

# para ir buscar o resultado de players no csv precisamos da data do jogo e do nome dos dois times que estão jogando
# temos a table events conectada por event_id, com isso temos  o home_team_id e away_team_id na table events e com esses id vamos
# para a table teams e pegamos os nomes dos times, com esses dados vamos buscar o match do jogo procurando no csv

# o csv tem essa estrutura
# gameid,datacompleteness,url,league,year,split,playoffs,date,game,patch,participantid,side,position,playername,playerid,teamname,teamid,champion,ban1,ban2,ban3,ban4,ban5,pick1,pick2,pick3,pick4,pick5,gamelength,result,kills,deaths,assists,teamkills,teamdeaths,doublekills,triplekills,quadrakills,pentakills,firstblood,firstbloodkill,firstbloodassist,firstbloodvictim,team kpm,ckpm,firstdragon,dragons,opp_dragons,elementaldrakes,opp_elementaldrakes,infernals,mountains,clouds,oceans,chemtechs,hextechs,dragons (type unknown),elders,opp_elders,firstherald,heralds,opp_heralds,void_grubs,opp_void_grubs,firstbaron,barons,opp_barons,atakhans,opp_atakhans,firsttower,towers,opp_towers,firstmidtower,firsttothreetowers,turretplates,opp_turretplates,inhibitors,opp_inhibitors,damagetochampions,dpm,damageshare,damagetakenperminute,damagemitigatedperminute,damagetotowers,wardsplaced,wpm,wardskilled,wcpm,controlwardsbought,visionscore,vspm,totalgold,earnedgold,earned gpm,earnedgoldshare,goldspent,gspd,gpr,total cs,minionkills,monsterkills,monsterkillsownjungle,monsterkillsenemyjungle,cspm,goldat10,xpat10,csat10,opp_goldat10,opp_xpat10,opp_csat10,golddiffat10,xpdiffat10,csdiffat10,killsat10,assistsat10,deathsat10,opp_killsat10,opp_assistsat10,opp_deathsat10,goldat15,xpat15,csat15,opp_goldat15,opp_xpat15,opp_csat15,golddiffat15,xpdiffat15,csdiffat15,killsat15,assistsat15,deathsat15,opp_killsat15,opp_assistsat15,opp_deathsat15,goldat20,xpat20,csat20,opp_goldat20,opp_xpat20,opp_csat20,golddiffat20,xpdiffat20,csdiffat20,killsat20,assistsat20,deathsat20,opp_killsat20,opp_assistsat20,opp_deathsat20,goldat25,xpat25,csat25,opp_goldat25,opp_xpat25,opp_csat25,golddiffat25,xpdiffat25,csdiffat25,killsat25,assistsat25,deathsat25,opp_killsat25,opp_assistsat25,opp_deathsat25
# LOLTMNT03_179647,complete,,LFL2,2025,Winter,0,2025-01-11 11:11:24,1,15.01,1,Blue,top,PatkicaA,oe:player:c659697694306de62d978569b84c344,IziDream,oe:team:84bc703e28859788770611d94cf02ac,Gnar,Vi,Skarner,Corki,K'Sante,Sylas,,,,,,1592,0,1,2,1,3,13,0,0,0,0,0,0,0,1,0.1131,0.6030,,,,,,,,,,,,,,,,,,,,,0,0,,,,,,,,,,0,1,20156,759.6482,0.40197,681.2186,629.7362,7451,9,0.3392,2,0.0754,3,17,0.6407,10668,7145,269.2839,0.289981,9793,,,234,234,0,,,8.8191,3058,4466,75,3394,4603,79,-336,-137,-4,0,0,1,1,0,0,4531,6777,119,5372,6968,125,-841,-191,-6,0,0,1,1,2,0,6473,9072,154,7012,9562,154,-539,-490,0,1,1,2,2,2,2,9244,12552,217,9020,12553,200,224,-1,17,1,1,2,2,4,2
# LOLTMNT03_179647,complete,,LFL2,2025,Winter,0,2025-01-11 11:11:24,1,15.01,2,Blue,jng,Joinze,oe:player:dbdc61a1c41acedcbc7d399727155ac,IziDream,oe:team:84bc703e28859788770611d94cf02ac,Maokai,Vi,Skarner,Corki,K'Sante,Sylas,,,,,,1592,0,0,3,1,3,13,0,0,0,0,0,0,0,0,0.1131,0.6030,,,,,,,,,,,,,,,,,,,,,0,1,,,,,,,,,,0,0,4963,187.0477,0.0989769,906.9724,954.6106,0,7,0.2638,7,0.2638,6,29,1.0930,7429,3906,147.2111,0.15852,7200,,,143,11,132,,,5.3894,2977,3153,62,3451,3687,71,-474,-534,-9,0,0,0,0,1,0,4461,5316,97,5289,5332,91,-828,-16,6,0,0,0,0,3,0,5668,6978,118,7357,8317,133,-1689,-1339,-15,0,1,1,0,5,0,7040,8877,139,9403,10321,157,-2363,-1444,-18,0,1,2,1,7,0
# ....
# LOLTMNT03_179647,complete,,LFL2,2025,Winter,0,2025-01-11 11:11:24,1,15.01,100,Blue,team,,,IziDream,oe:team:84bc703e28859788770611d94cf02ac,,Vi,Skarner,Corki,K'Sante,Sylas,Maokai,Jinx,Leona,Hwei,Gnar,1592,0,3,13,5,3,13,0,0,0,0,0,,,,0.1131,0.6030,0,0,2,0,2,0,0,0,0,0,0,,0,0,0,0,1,0,6,0,0,1,0,1,0,3,9,0,0,1,8,0,2,50143,1889.8116,,2908.7940,2642.4874,7784,74,2.7889,24,0.9045,23,158,5.9548,42255,24639,928.6055,,38793,-0.112676,-3.34,,731,144,,,32.9774,14266,18519,313,15988,19181,345,-1722,-662,-32,0,0,1,1,1,0,21498,28270,496,25335,28739,512,-3837,-469,-16,0,0,3,3,9,0,29475,38659,656,35378,42861,702,-5903,-4202,-46,2,3,6,6,14,2,39226,50120,845,46192,55920,875,-6966,-5800,-30,3,5,10,10,26,3
# LOLTMNT03_179647,complete,,LFL2,2025,Winter,0,2025-01-11 11:11:24,1,15.01,200,Red,team,,,Team Valiant,oe:team:71bd93fd1eab2c2f4ba60305ecabce2,,Yone,Viktor,Aurora,Nocturne,Jarvan IV,Varus,Ivern,Braum,Renekton,Orianna,1592,1,13,3,36,13,3,0,0,0,0,1,,,,0.4899,0.6030,1,2,0,2,0,1,0,1,0,0,0,,0,0,1,1,0,6,0,1,1,0,1,0,1,9,3,1,1,8,1,2,0,53681,2023.1533,,2527.6508,2272.2362,22322,78,2.9397,35,1.3191,33,196,7.3869,53936,36320,1368.8442,,43425,0.112676,3.34,,753,169,,,34.7487,15988,19181,345,14266,18519,313,1722,662,32,1,1,0,0,0,1,25335,28739,512,21498,28270,496,3837,469,16,3,9,0,0,0,3,35378,42861,702,29475,38659,656,5903,4202,46,6,14,2,2,3,6,46192,55920,875,39226,50120,845,6966,5800,30,10,26,3,3,5,10

# gameid do csv não significada nada, então precisamos buscar  os times no exemplo acima temos IziDream vs Team Valiant
# repare que os times são os participandid 100 e 200 do gameid, e temos a data do jogo também, league e nas linhas de players
# que são participand id de 1 a 10, temos todo o resultado do player no jogo incluindo kills deaths and assistis

# apos dar o match no jogo só precisamos comparar a selection e bet_line com o resultado real e atualizar o banco com o resultado
# igual fizemos para totals

# no fim do script mostrar quantas apostas pendentes temos
# quebrar em apostas pendentes de 7 dias para mais antigas
# e apostas pendentes de 1 dia anterior ao rodado o codigo e futuras
