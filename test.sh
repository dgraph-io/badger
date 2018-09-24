l=$(go list ./...)
for x in $l; do
  echo "Testing package $x"
  # go test -race -v $x
  go test -v $x || exit 1
done
