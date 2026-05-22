from api_telemetria import models
from rest_framework import status, viewsets
from api_telemetria.api import serializers
from drf_yasg.utils import swagger_auto_schema

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser
from api_telemetria.api.services import processar_csv_medicoes
from rest_framework.decorators import action
from django.shortcuts import get_object_or_404
from django.db.models import F

from django.contrib.auth.models import User
from django.contrib.auth import authenticate
from rest_framework.authtoken.models import Token
from rest_framework.permissions import IsAuthenticated

class VeiculoViewSet(viewsets.ModelViewSet):
    queryset = models.Veiculo.objects.all()
    serializer_class = serializers.VeiculoSerializer
    permision_classes = [IsAuthenticated]
    
    # decoradores para documentação do swagger, descrevendo cada endpoint e os tipos de resposta esperados  
    
    @swagger_auto_schema( 
        operation_description="Retorna todas os tipos de veículos",
        responses={200: serializers.VeiculoSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    # Retorna todas os tipos de veículos, utilizando o serializer para formatar a resposta, e o decorador para documentar a operação no swagger
    # Método GET, endpoint /veiculo/ (listagem de veículos)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de veículo",
        responses={201: serializers.VeiculoSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    # Cria um novo registro de veículo, utilizando o serializer para validar e salvar os dados, e o decorador para documentar a operação no swagger
    # Método POST, endpoint /veiculo/ (criação de veículo)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de veículo conforme o ID fornecido",
        responses={200: serializers.VeiculoSerializer(many=False)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    # Retorna o registro de veículo conforme o ID fornecido, utilizando o serializer para formatar a resposta, e o decorador para documentar a operação no swagger
    # Método GET ID, endpoint /veiculo/{id}/ (detalhes de um veículo específico)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de veículo conforme o dados passados para o ID fornecido",
        responses={200: serializers.VeiculoSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    # Altera o registro de veículo conforme os dados passados para o ID fornecido, utilizando o serializer para validar e salvar os dados, e o decorador para documentar a operação no swagger
    # Método PUT ID, endpoint /veiculo/{id}/ (atualização de um veículo específico)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de veículo conforme o ID fornecido",
        responses={204: serializers.VeiculoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
    # Deleta o registro de veículo conforme o ID fornecido, utilizando o decorador para documentar a operação no swagger
    # Método DELETE ID, endpoint /veiculo/{id}/ (remoção de um veículo específico)
    
class MarcaViewSet(viewsets.ModelViewSet):
    queryset = models.Marca.objects.all()
    serializer_class = serializers.MarcaSerializer
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de marcas de veículos",
        responses={200: serializers.MarcaSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de marca de veículo",
        responses={201: serializers.MarcaSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de marca conforme o ID fornecido",
        responses={200: serializers.MarcaSerializer(many=False)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de marca conforme o dados passados para o ID fornecido",
        responses={200: serializers.MarcaSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de marca conforme o ID fornecido",
        responses={204: serializers.MarcaSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)

class ModeloViewSet(viewsets.ModelViewSet):
    queryset = models.Modelo.objects.all()
    serializer_class = serializers.ModeloSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de modelos de veículos",
        responses={200: serializers.ModeloSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de modelo de veículo",
        responses={201: serializers.ModeloSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de modelo de veículo conforme o ID fornecido",
        responses={200: serializers.ModeloSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de modelo de veículo conforme o dados passados para o ID fornecido",
        responses={200: serializers.ModeloSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de modelo de veículo conforme o ID fornecido",
        responses={204: serializers.ModeloSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)

class MedicaoVeiculoViewSet(viewsets.ModelViewSet):
    queryset = models.MedicaoVeiculo.objects.all()
    serializer_class = serializers.MedicaoVeiculoSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de medição de veículos específicos",
        responses={200: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de medição de veículos específicos",
        responses={201: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de medição de veículos específicos conforme o ID fornecido",
        responses={200: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de medição de veículos específicos conforme o dados passados para o ID fornecido",
        responses={200: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de medição de veículos específicos conforme o ID fornecido",
        responses={204: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
        
    @action(detail=True, methods=['get'], url_path='rel_medicoesporveiculo')
    def list_medicoes_por_veiculo_rel(self, request, pk=None):
        porveiculos = get_object_or_404(models.Veiculo, id=pk)
        medicoes = models.MedicaoVeiculo.objects.filter(VeiculoId = porveiculos)
        serializerMedicao = serializers.MedicaoVeiculoSerializer(medicoes, many=True)
        return Response(serializerMedicao.data)

class MedicaoViewSet(viewsets.ModelViewSet):
    queryset = models.Medicao.objects.all()
    serializer_class = serializers.MedicaoSerializer
    @swagger_auto_schema(
        operation_description="Retorna todas as informações de telemetria de tipos de medição",
        responses={200: serializers.MedicaoSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de medição de telemetria",
        responses={201: serializers.MedicaoSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de medição conforme o ID fornecido",
        responses={200: serializers.MedicaoSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de medição conforme o dados passados para o ID fornecido",
        responses={200: serializers.MedicaoSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de medição conforme o ID fornecido",
        responses={204: serializers.MedicaoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)

class UnidadeMedidaViewSet(viewsets.ModelViewSet):
    queryset = models.UnidadeMedida.objects.all()
    serializer_class = serializers.UnidadeMedidaSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de unidade de medida",
        responses={200: serializers.UnidadeMedidaSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de unidade de medida",
        responses={201: serializers.UnidadeMedidaSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de unidade de medida conforme o ID fornecido",
        responses={200: serializers.UnidadeMedidaSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de unidade de medida conforme o dados passados para o ID fornecido",
        responses={200: serializers.UnidadeMedidaSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de medição conforme o ID fornecido",
        responses={204: serializers.MedicaoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
    
class ImportarMedicoesCSVViewSet(APIView):
    parser_classes = [MultiPartParser, FormParser]
    
    def post(self, request, *args, **kwargs):
        serializer = serializers.UploadCSVSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        arquivo = serializer.validated_data['arquivo']
        
        try:
            resultado = processar_csv_medicoes(arquivo)
            
            return Response(
                {
                    "mensagem": "Arquivo processado com sucesso",
                    **resultado
                },
                status=status.HTTP_201_CREATED
            )
            
        except Exception as e:
            return Response(
                {
                    "erro": "Falha ao processar o arquivo",
                    "detalhes": str(e)
                },
                status=status.HTTP_400_BAD_REQUEST
            )    
    
class MedicaoVeiculoTempViewSet(viewsets.ModelViewSet):
    queryset = models.MedicaoVeiculoTemp.objects.all()
    serializer_class = serializers.MedicaoVeiculoTempSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de medição temporária de veículos específicos",
        responses={200: serializers.MedicaoVeiculoTempSerializer(many=True)},)
    
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

class DadosRelatorioViewSet(viewsets.ModelViewSet):
    queryset = models.MedicaoVeiculo.objects.all()
    serializer_class = serializers.MedicaoVeiculoSerializer

    @swagger_auto_schema(
        operation_description="Retorna relatório de medições com dados do veículo, modelo, marca e tipo de medição",
    )
    @action(detail=False, methods=['get'])
    
    def DadosRelatorio(self, request):
        dados = models.MedicaoVeiculo.objects.select_related(
            'MedicaoId__UnidadeMedida',
            'VeiculoId__ModeloId',
            'VeiculoId__MarcaId',
            
            ).annotate(
            Descricao=F('VeiculoId__Descricao'),
            Modelo=F('VeiculoId__ModeloId__Nome'),
            Marca=F('VeiculoId__MarcaId__Nome'),
            Tipo=F('MedicaoId__Tipo'),
            Simbolo=F('MedicaoId__UnidadeMedidaId__Nome'),
            
        ).values(
            'id',
            'Data',
            'Descricao',
            'Modelo',
            'Marca',
            'Tipo',
            'Simbolo',
            'Valor',
        )
        serializer = serializers.DadosRelatorioSerializer(dados, many=True)
        return Response(serializer.data)

class UserViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.UserSerializer  
    queryset = User.objects.all()
    
class LoginViewSet(viewsets.ViewSet):
    
    def create(self, request):  
        
        serializer = serializers.LoginSerializer(data=request.data)
        
        if serializer.is_valid():
            
            username = serializer.validated_data['username']
            password = serializer.validated_data['password']
            user = authenticate(username=username, password=password)
        
            if user is not None:
                
                token, created = Token.objects.get_or_create(user=user)
                return Response({
                    'token': token.key,
                    'user': serializers.UserSerializer(user).data},status=status.HTTP_200_OK)
            
        return Response({'error': 'Credenciais inválidas'}, status=status.HTTP_400_BAD_REQUEST)
    