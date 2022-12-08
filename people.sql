create table people (
	person_id_date varchar primary key,
	person_id integer not null,
	"date" date not null,
	english_name varchar ( 50 ) not null,
	japanese_name varchar ( 50 ),
	mal_link varchar ( 255 ) not null,
	image_link varchar ( 255 ),
	favorites integer not null
);
