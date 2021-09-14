{
	"targets": [
		{
			"target_name": "shmjs",
			"sources": [
				"bifrost/shmjs.cpp"
			],
			"include_dirs": [
				"<!(node -e \"require('nan')\")"
			]
		}
	]
}
