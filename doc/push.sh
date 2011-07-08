make html
git checkout gh-pages
mv ./html/* ..
pushd ..
git add -u
git commit -m "Bump"
git push
popd
git checkout master