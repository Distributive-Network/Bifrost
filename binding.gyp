{
  "targets": [
    {
      "target_name": "shmjs",
      "sources": [
        "shmjs.cpp"
      ],
      "include_dirs": [
        "<!(node -e \"require('nan')\")"
      ],
      "libraries" : [
        "-lrt"
      ]
    }
  ]
}
