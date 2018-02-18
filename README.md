# Seguro DB

Fast and reliable database, optimised for read operations.

TODO: Additional security layers, mirroing, versioning, session and long-term encryption will be implemented soon.

### Database options

- Journal eras (`usize`)
- Preallocated memory (`u64`)
- Extend threshold in % (`u8`)

### Database properties

- Version (`u32`)
- Used memory (`u64`)

### get operation

- Check cache
- Check journal
- Read from memmap

### commit operation

- Create and push new journal era

### flush operation

- Create virtual commit from final journal eras
- Delete journal eras
- Copy content of virtual commit to memmap
- Delete virtual commit

### rollback operation

- Pop and delete journal era

### recover operation

- If valid virtual commit exists copy it to memmap and delete
- Delete all invalid journal eras
