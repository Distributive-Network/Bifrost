{
  "targets": [
    {
      "target_name": "shmjs",
      "sources": [
        "src/shmjs.cpp"
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
