import boto3
from flask import Flask, request, jsonify
import uuid
from datetime import datetime, timedelta
import schedule
import time
import threading
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import io



propiedades_cliente=['razon_social', 'nombre_comercial', 'correo']
propiedades_domicilio=['Domicilio', 'Colonia', 'Municipio', 'Estado', 'Tipo_de_direccion']
propiedades_producto = ['Nombre', 'Unidad_de_medida', 'Precio_base']

def suscribir_sns(correo):
    response = sns.subscribe(
        TopicArn=TOPIC_ARN,
        Protocol='email',
        Endpoint=correo
    )
    return response


def enviar_sns(correo, enlace_descarga):
    mensaje = f'Se ha generado una nueva nota de venta. Puedes descargarla aquí: {enlace_descarga}'
    subject = 'Nueva Nota de Venta Generada'

    # Publica un mensaje en el tema SNS
    response = sns.publish(
        TopicArn=TOPIC_ARN,
        Message=mensaje,
        Subject=subject,
        MessageAttributes={
            'email': {
                'DataType': 'String',
                'StringValue': correo
            }
        }
    )
    return response

@app.route('/clientes', methods=['POST'])
def crear_cliente():
    try:
        data = request.json
        for prop in propiedades_cliente:
                if prop not in data:
                    return jsonify({'mensaje': 'Faltan una o más propiedades'}), 400
        cliente_id = str(uuid.uuid4())
        cliente = {
            'clienteid': cliente_id,
            'razon_social': data['razon_social'],
            'nombre_comercial': data['nombre_comercial'],
            'correo': data['correo']
        }
        clientes_table.put_item(Item=cliente)

        suscribir_sns(data['correo'])

        return jsonify({'mensaje': 'Cliente creado correctamente', 'id': cliente_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/clientes', methods=['GET'])
def obtener_clientes():
    response = clientes_table.scan()
    clientes = response.get('Items', [])
    return jsonify(clientes)

@app.route('/clientes/<string:id>', methods=['GET'])
def get_cliente(id):
    try:
        response = clientes_table.get_item(Key={'clienteid': id})
        if 'Item' in response:
            cliente = response['Item']
            return jsonify({'Cliente:':cliente}), 200
        else:
            return jsonify({'message': 'No se econtró un cliente con ese id'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/clientes/<string:id>', methods=['PUT'])
def actualizar_cliente(id):
    try:
        data = request.json
        for prop in propiedades_cliente:
            if prop not in data:
                return jsonify({'mensaje': 'Faltan una o más propiedades'}), 400
        response = clientes_table.get_item(Key={'clienteid': id})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un cliente con ese id'}), 404
        clientes_table.update_item(
            Key={'clienteid': id},
            UpdateExpression='SET razon_social = :razon_social, nombre_comercial = :nombre_comercial, correo = :correo',
            ExpressionAttributeValues={
                ':razon_social': data['razon_social'],
                ':nombre_comercial': data['nombre_comercial'],
                ':correo': data['correo']
            }
        )
        return jsonify({'mensaje': 'Cliente actualizado correctamente'}),200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/clientes/<string:id>', methods=['DELETE'])
def eliminar_cliente(id):
    try:
        response = clientes_table.get_item(Key={'clienteid': id})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un cliente con ese id'}), 404
        clientes_table.delete_item(Key={'clienteid': id})
        return jsonify({'mensaje': 'Cliente eliminado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/domicilios/<string:clienteid>', methods=['POST'])
def crear_domicilio(clienteid):
    try:
        response = clientes_table.get_item(Key={'clienteid': clienteid})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un cliente con ese id'}), 404
        data = request.json
        for prop in propiedades_domicilio:
                if prop not in data:
                    return jsonify({'mensaje': 'Faltan una o más propiedades'}), 400
        tipo =  data['Tipo_de_direccion']
        if tipo != ("facturacion" or "envio"):
            return jsonify({'mensaje': 'El tipo de direccion solo puede tener los valores, "facturacion" ó "envio"'}), 400
        domicilio = {
            'clienteid': clienteid,
            'Domicilio': data['Domicilio'],
            'Colonia': data['Colonia'],
            'Municipio': data['Municipio'],
            'Estado' : data['Estado'],
            'Tipo_de_direccion' : data['Tipo_de_direccion']
        }
        domicilios_table.put_item(Item=domicilio)

        return jsonify({'mensaje': 'Domicilio creado correctamente', 'id': clienteid}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/domicilios', methods=['GET'])
def obtener_domicilios():
    response = domicilios_table.scan()
    domicilios = response.get('Items', [])
    return jsonify(domicilios)

@app.route('/domicilios/<string:id>', methods=['GET'])
def get_domicilio(id):
    try:
        response = domicilios_table.get_item(Key={'clienteid': id})
        if 'Item' in response:
            domicilio = response['Item']
            return jsonify({'Cliente:':domicilio}), 200
        else:
            return jsonify({'message': 'No se econtró un domicilio con ese id'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/domicilios/<string:id>', methods=['PUT'])
def actualizar_domicilio(id):
    try:
        data = request.json
        for prop in propiedades_domicilio:
            if prop not in data:
                return jsonify({'mensaje': 'Faltan una o más propiedades'}), 400
        response = domicilios_table.get_item(Key={'clienteid': id})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un domicilio con ese id'}), 404
        domicilios_table.update_item(
            Key={'clienteid': id},
            UpdateExpression='SET Domicilio = :Domicilio, Colonia = :Colonia, Municipio = :Municipio, Estado = :Estado, Tipo_de_direccion = :Tipo_de_direccion',
            ExpressionAttributeValues={
                ':Domicilio': data['Domicilio'],
                ':Colonia': data['Colonia'],
                ':Municipio': data['Municipio'],
                ':Estado' : data['Estado'],
                ':Tipo_de_direccion' : data['Tipo_de_direccion']
            }
        )
        return jsonify({'mensaje': 'Domicilio actualizado correctamente'}),200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/domicilios/<string:id>', methods=['DELETE'])
def eliminar_domicilio(id):
    try:
        response = domicilios_table.get_item(Key={'clienteid': id})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un domicilio con ese id'}), 404
        domicilios_table.delete_item(Key={'clienteid': id})
        return jsonify({'mensaje': 'Domicilio eliminado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/productos', methods=['POST'])
def crear_producto():
    try:
        data = request.json
        for prop in propiedades_producto:
                if prop not in data:
                    return jsonify({'mensaje': 'Faltan una o más propiedades'}), 400
        producto_id = str(uuid.uuid4())
        producto = {
            'productoid': producto_id,
            'Nombre': data['Nombre'],
            'Unidad_de_medida': data['Unidad_de_medida'],
            'Precio_base': data['Precio_base']
        }
        productos_table.put_item(Item=producto)

        return jsonify({'mensaje': 'Producto creado correctamente', 'id': producto_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    
@app.route('/productos', methods=['GET'])
def obtener_productos():
    response = productos_table.scan()
    productos = response.get('Items', [])
    return jsonify(productos)

@app.route('/productos/<string:id>', methods=['GET'])
def get_producto(id):
    try:
        response = productos_table.get_item(Key={'productoid': id})
        if 'Item' in response:
            producto = response['Item']
            return jsonify({'Producto:':producto}), 200
        else:
            return jsonify({'message': 'No se econtró un producto con ese id'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/productos/<string:id>', methods=['PUT'])
def actualizar_producto(id):
    try:
        data = request.json
        for prop in propiedades_producto:
            if prop not in data:
                return jsonify({'mensaje': 'Faltan una o más propiedades'}), 400
        response = productos_table.get_item(Key={'productoid': id})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un producto con ese id'}), 404
        productos_table.update_item(
            Key={'productoid': id},
            UpdateExpression='SET Nombre = :Nombre, Unidad_de_medida = :Unidad_de_medida, Precio_base = :Precio_base',
            ExpressionAttributeValues={
                ':Nombre': data['Nombre'],
                ':Unidad_de_medida': data['Unidad_de_medida'],
                ':Precio_base': data['Precio_base']
            }
        )
        return jsonify({'mensaje': 'Producto actualizado correctamente'}),200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/productos/<string:id>', methods=['DELETE'])
def eliminar_producto(id):
    try:
        response = productos_table.get_item(Key={'productoid': id})
        if 'Item' not in response:
            return jsonify({'message': 'No se econtró un producto con ese id'}), 404
        productos_table.delete_item(Key={'productoid': id})
        return jsonify({'mensaje': 'Producto eliminado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def generar_pdf(nota_venta, contenido):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    
    c.drawString(100, 750, f"Nota de Venta: {nota_venta['notaid']}")
    c.drawString(100, 730, f"Cliente: {nota_venta['cliente_id']}")
    c.drawString(100, 710, f"Dirección Facturación: {nota_venta['direccion_facturacion']}")
    c.drawString(100, 690, f"Dirección Envío: {nota_venta['direccion_envio']}")
    c.drawString(100, 670, f"Total: {nota_venta['total_nota']}")

    y_position = 650
    for item in contenido:
        item_productoid = item['productoid']
        item_cantidad = item['cantidad']
        item_preciobase = item['Precio_base']
        c.drawString(100, y_position, f"Producto: {item_productoid}, Cantidad: {item_cantidad}, Precio: {item_preciobase}")
        y_position -= 20

    c.showPage()
    c.save()

    buffer.seek(0)
    return buffer


def subir_s3(buffer, filename):
    s3.put_object(Bucket=BUCKET_NAME, Key=f'notas_venta/{filename}', Body=buffer, ContentType='application/pdf', ACL='public-read')
    enlace_descarga = f"https://{BUCKET_NAME}.s3.amazonaws.com/notas_venta/{filename}"
    return enlace_descarga

@app.route('/notas_venta', methods=['POST'])
def crear_nota_venta():
    data = request.json

    nota_id = str(uuid.uuid4())


    cliente_id = data.get('cliente_id')
    cliente_email = data.get('cliente_email')
    direccion_facturacion = data.get('direccion_facturacion')
    direccion_envio = data.get('direccion_envio')
    total_nota = data.get('total_nota')
    contenido = data.get('contenido', [])

    if not cliente_id or not cliente_email or not direccion_facturacion or not direccion_envio or not total_nota or not contenido:
        return jsonify({'error': 'Faltan datos para crear la nota de venta'}), 400

    nota_venta = {
        'notaid': nota_id,
        'cliente_id': cliente_id,
        'direccion_facturacion': direccion_facturacion,
        'direccion_envio': direccion_envio,
        'total_nota': total_nota
    }

    notas_venta_table.put_item(Item=nota_venta)

    for item in contenido:
        contenido_id = str(uuid.uuid4())
        importe = item['cantidad'] * item['Precio_base']
        contenido_item = {
            'contenidoid': contenido_id,
            'notaid': nota_id,
            'productoid': item['productoid'],
            'cantidad': item['cantidad'],
            'Precio_base': item['Precio_base'],
            'importe': importe
        }
        contenido_nota_table.put_item(Item=contenido_item)

    buffer_pdf = generar_pdf(nota_venta, contenido)

    nombre = f'nota_{nota_id}.pdf'
    enlace_descarga = subir_s3(buffer_pdf, nombre)

    enviar_sns(cliente_email, enlace_descarga)

    return jsonify({'mensaje': 'Nota de venta creada correctamente', 'notaid': nota_id, 'enlace_descarga': enlace_descarga}), 201

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
