#!/bin/bash
# Downloads paper issues from the Valley of the Shadow project.

papers=(rv ss vv fr sd vs vr)

cd mirrors
for paper in ${papers[*]}
do
	wget -nc --recursive -A .xml "http://valley.lib.virginia.edu/news-calendar/?paper=$paper"
done

echo "Done."
