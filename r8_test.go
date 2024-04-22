package main

import (
	"os"
	"testing"
)

const numOfCPUs = 4

func TestReadEntireFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_r8.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: \n%+v", err)
	}
	defer os.Remove(tmpFile.Name())

	testData := getTestData()
	_, err = tmpFile.WriteString(testData)
	if err != nil {
		t.Fatalf("Failed to write to temp file: \n%+v", err)
	}

	fileParts, err := splitFile(tmpFile.Name(), numOfCPUs)
	if err != nil {
		t.Fatalf("Failed to split temp file: \n%+v", err)
	}

	fileStats, err := tmpFile.Stat()
	if err != nil {
		t.Fatalf("Failed to get temp file stats: \n%+v", err)
	}

	fileSize := fileStats.Size()
	partsSize := int64(0)

	for _, fp := range fileParts {
		partsSize += fp.size
	}

	if partsSize != fileSize {
		t.Errorf("Size of file parts (%d) does not equal file size (%d)", partsSize, fileSize)
	}
}

func getTestData() string {
	return `Belas;48.7
	Hulin;-95.1
	Neuwied;50.2
	Bellinzona;10.3
	Volgorechensk;-52.6
	Narón;15.0
	Sibilia;-63.3
	Bedum;9.0
	Qyzylorda;-10.6
	Muzambinho;67.7
	Ząbki;-26.8
	Madnūr;-37.5
	Kornwestheim;13.4
	Bom Jesus dos Perdões;55.0
	Benjamin Constant;26.5
	Tiaong;-98.7
	North Whitehall;3.7
	La Flèche;74.7
	Mezőkövesd;-75.3
	Mahazony;-38.1
	Kawachinagano;-76.4
	Lamarão;59.9
	Neiva;-81.8
	White City;28.7
	Detva;73.1
	Toffo;25.8
	Kamenka;-40.8
	Oulad Friha;94.2
	Jishi;8.6
	Manzanares el Real;61.2
	Tom Price;26.2
	Lusanga;86.8
	Ban Bo Luang;-47.6
	Ansonia;1.1
	Scherpenzeel;-79.6
	Uryupinsk;-25.4
	La Primavera;9.4
	Uozu;45.0
	Sarmiento;24.1
	Gongguan;-38.6
	Qingzhou;-89.6
	Tietê;-60.3
	Khujner;1.8
	Serpukhov;-53.6
	Tulare;54.8
	Dudelange;-69.1
	Ban Bang Phun;-72.0
	Timonium;-79.6
	Sandur;85.1
	Naduvalūr;-80.7
	Aungban;-76.0
	Cagdianao;-72.9
	Busovača;74.3
	Uchturpan;-99.3
	Itzer;76.7
	Lambesc;-77.3
	Vienne;6.9
	Dresden;37.9
	Guamo;85.1
	Castiglione delle Stiviere;71.0
	Bhogpur;-31.1
	Ghogardīha;2.4
	Boaz;-93.4
	Broussard;-56.4
	Hazel Crest;-81.7
	Bridlington;67.5
	Quzanlı;-24.6
	Sodankylä;-7.3
	Panazol;-63.2
	Taverny;0.3
	Ingolstadt;66.3
	Sainte-Geneviève-des-Bois;-38.6
	Ferozepore;-44.7
	Anse-à-Veau;-38.5
	Vanadzor;-44.9
	Manerbio;2.2
	Hassa;-63.4
	Haiku-Pauwela;11.0
	Bihār;-27.8
	Parasurāmpūr;49.8
	Ad Darb;-90.7
	Were Īlu;51.5
	Cañada de Gómez;-30.0
	Tiruvādi;37.8
	Sarpang;-65.9
	Shanhūr;-57.1
	Wyke;87.6
	West Auckland;-41.8
	Gajhara;14.2
	Oissel;-25.0
	Kondapalle;50.7
	Aguadilla;-53.7
	Qiryat Yam;-8.5
	Premiá de Mar;54.7
	Douar Souk L‘qolla;-52.2
	Rogers;-90.5
	Požega;-4.2
	Chaudfontaine;52.3
	Cunha Porã;79.9
	Nanwucun;2.1
	Uricani;-70.0
	Unchahra;-30.7
	Douar Bni Malek;95.5
	Sebnitz;-77.3
	Rocky Mount;-2.3
	Gangāpur;31.6
	Hyderābād;2.1
	Eqlīd;-17.1
	Oftersheim;89.2
	Maheshrām;67.7
	Piñan;-45.1
	Missouri City;90.7
	Mława;-52.0
	Ibitinga;-89.4
	Jonnagiri;-63.2
	Hainichen;-49.3
	Al Ḩudaydah;51.8
	Mariano Roque Alonso;5.6
	Escada;22.2
	Yeşilhisar;50.2
	Qornet Chahouâne;-81.2
	Lichtenfels;-95.5
	Anoviara;63.9
	Orós;-77.7
	Mikashevichy;69.2
	Urganch;91.9
	Rutesheim;79.5
	Freienbach;68.0
	Aleksin;19.2
	Caudete;31.6
	Aldine;78.9
	Golub-Dobrzyń;46.6
	Sidi Bousber;-15.0
	Puerto America;-72.6
	Al Balyanā;40.9
	Cape Coast;98.6
	Kakuma;-96.5
	Friedrichshafen;-97.5
	Altepexi;49.0
	Jauharabad;-14.8
	Bishops Stortford;-53.1
	Assèmini;-70.4
	Higashiōmi;52.9
	Arroio do Tigre;-54.3
	Van;9.3
	Chaguanas;-57.9
	Wenxicun;-74.4
	Coatepeque;23.7
	Raisāri;-65.0
	Al Madad;20.7
	Liantangcun;36.0
	Barnāon;21.0
	Tanque Novo;98.9
	Mering;-31.7
	Severobaykalsk;72.1
	Solhan;55.0
	Paragaticherla;-49.9
	Codigoro;-56.4
	Lumding;56.4
	Port Jervis;-8.4
	Waterville;95.5
	Flero;54.7
	Campagna;90.1
	Eksjö;82.0
	White City;-43.8
	Edwardsville;63.9
	Jhundo;-88.5
	Kapfenberg;-92.9
	Yamanouchi;-12.7
	Maliāl;-51.1
	Yingcheng;-1.3
	Ferrara;-74.9
	Paittūr;-97.6
	Villarrobledo;-58.4
	Longbridge;-31.5
	Cheshunt;7.3
	Vélez;79.6
	Roman;17.3
	Guardamar del Segura;-69.2
	Porangatu;-98.7
	Pesca;21.2
	Calvillo;98.4
	Sunset Hills;29.2
	Derhachi;1.0
	Highland Park;-48.0
	Nejo;81.3
	Baton Rouge;24.5
	Schofield Barracks;-92.8
	Râmnicu Vâlcea;-17.3
	Cerrillos;-16.2
	Hobro;28.9
	Baghambarpur;92.7
	Lucknow;11.5
	Delray Beach;63.5
	Kirensk;6.5
	Sendamangalam;-18.6
	Picture Rocks;10.3
	Puducherry;99.6
	Ephrata;72.9
	Alcobaça;43.7
	Otočac;-74.2
	Santa Maria da Boa Vista;-60.7
	Maxixe;69.6
	Simi Valley;95.3
	Sidi Boushab;83.4
	Cocotitlán;4.7
	Frodsham;-78.0
	Baruāri;-77.6
	Kofelē;-32.0
	Xiaoli;37.6
	Shāhedshahr;14.4
	Corsico;58.3
	Heusenstamm;65.5
	Uruçuca;92.8
	Natagaima;67.7
	Saharefo;-56.1
	College;-5.6
	Balandougou;59.2
	Dhubaria;97.4
	Dornava;-50.1
	Justice;5.8
	Borehamwood;-55.8
	Nakūr;3.7
	Fenglu;11.0
	North Strabane;11.8
	Tyāmagondal;10.4
	Pharr;-87.7
	Warsop;-68.1
	Omaruru;48.1
	Tōin;-53.3
	Piraziz;22.9
	Kobyłka;-30.7
	Bijie;92.0
	Kushtagi;56.4
	Cavriago;-34.1
	Pāthardi;-85.7
	Mtsensk;70.3
	Eloy;-45.7
	Morbi;-30.3
	Santa Teresinha (2);68.4
	Saint-Sébastien-sur-Loire;6.1
	Shtime;84.9
	Xanxerê;-65.8
	Longaví;41.0
	Fairview Park;-29.4
	Greene;-52.6
	Centennial;77.1
	Alfajayucan;-43.6
	Matigou;-38.1
	Ilsfeld;34.7
	Hyderābād;8.0
	Axapusco;23.6
	Cubulco;-45.2
	Tazzarine;-88.9
	Burauen;43.7
	Payson;73.0
	Brokopondo;-70.8
	Zeitz;49.5
	Wallan;30.3
	Vologda;73.5
	Zarumilla;90.5
	Chikusei;28.3
	Plouzané;-84.2
	Löbau;25.1
	Chautāpal;9.6
	Ballenger Creek;44.8
	Sam Phran;-1.6
	Pudong;-18.5
	Sanmenxia;-91.7
	Akbarpur;-29.4
	Brownwood;88.4
	Atlatlahucan;-28.5
	Genthin;-67.1
	Foum Jam’a;-31.8
	Godomè;85.8
	Kānkuria;-9.7
	Agdangan;47.5
	Bela Simri;-86.5
	Anajatuba;-1.1
	Slyudyanka;4.7
	Tüp;-1.5
	Marovandrika;-27.8
	Tulagi;51.2
	As Samāwah;-34.0
	Karakax;76.4
	Boralday;33.2
	Chili;-4.8
	Albertville;78.0
	Almus;-8.6
	Jahrom;-93.8
	Plan-de-Cuques;44.6
	Büyükçekmece;3.4
	Khvāf;-76.1
	Hannoversch Münden;12.3
	Boom;42.3
	Port Hope;-79.4
	Ryūyō;19.7
	Teotepeque;74.4
	Vilkaviškis;7.8
	Kempsey;64.1`
}
