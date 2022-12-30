MAIN_DIR="."
isort -rc $MAIN_DIR
autoflake -r --in-place --remove-unused-variables $MAIN_DIR
black $MAIN_DIR --line-length 80
