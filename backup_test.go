func TestBackupBitClear(t *testing.T) {
 dir, err := ioutil.TempDir("", "badger-test")
 require.NoError(t, err)
 defer removeDir(dir)
 
 opt := getTestOptions(dir)
 opt.ValueThreshold = 10 // This is important
 db, err := Open(opt)
 require.NoError(t, err)
 
 key := []byte("foo")
 val := []byte(fmt.Sprintf("%0100d", 1))
 require.Greater(t, len(val), db.opt.ValueThreshold)
 
 err = db.Update(func(txn *Txn) error {
     e := NewEntry(key, val)
     // Value > valueTheshold so bitValuePointer will be set.
     return txn.SetEntry(e)
 })
 require.NoError(t, err)
 
 // Use different directory.
 dir, err = ioutil.TempDir("", "badger-test")
 require.NoError(t, err)
 defer removeDir(dir)
 
 bak, err := ioutil.TempFile(dir, "badgerbak")
 require.NoError(t, err)
 _, err = db.Backup(bak, 0)
 require.NoError(t, err)
 require.NoError(t, bak.Close())
 require.NoError(t, db.Close())
 
 opt = getTestOptions(dir)
 opt.ValueThreshold = 200 // This is important.
 db, err = Open(opt)
 require.NoError(t, err)
 defer db.Close()
 
 bak, err = os.Open(bak.Name())
 require.NoError(t, err)
 defer bak.Close()
 
 require.NoError(t, db.Load(bak, 16))
 
 require.NoError(t, db.View(func(txn *Txn) error {
     e, err := txn.Get(key)
     require.NoError(t, err)
     v, err := e.ValueCopy(nil)
     require.NoError(t, err)
     require.Equal(t, val, v)
     return nil
 }))
}
