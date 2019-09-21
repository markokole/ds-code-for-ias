STACK_NAME = casestudy

deploy-cf:
	aws cloudformation deploy \
		--template-file template.yml \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			BucketName=$(BUCKET_NAME)

deploy-data:
	aws s3 cp accommodation.json s3://$(BUCKET_NAME)/snapshots/accommodation/date=2019-08-01/accommodation.json
	aws athena start-query-execution \
		--query-string "MSCK REPAIR TABLE accommodation" \
		--query-execution-context Database=trivago \
		--result-configuration OutputLocation=s3://$(BUCKET_NAME)/output
	aws s3 cp unique_selling_points.json s3://$(BUCKET_NAME)/snapshots/unique_selling_points/date=2019-08-01/unique_selling_points.json
	aws athena start-query-execution \
		--query-string "MSCK REPAIR TABLE unique_selling_points" \
		--query-execution-context Database=trivago \
		--result-configuration OutputLocation=s3://$(BUCKET_NAME)/output

deploy: deploy-cf deploy-data

delete-cf:
	aws cloudformation delete-stack \
		--stack-name $(STACK_NAME)

delete-data:
	aws s3 rm s3://$(BUCKET_NAME) --recursive

delete: delete-data delete-cf