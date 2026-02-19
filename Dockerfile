FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY extract.py .
COPY transform.py .
COPY load.py .
COPY lambda_function.py .

CMD ["lambda_function.lambda_handler"]