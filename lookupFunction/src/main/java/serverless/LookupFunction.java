package serverless;

import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.gson.Gson;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

public class LookupFunction implements HttpFunction {

    @Override
    public void service(HttpRequest request, HttpResponse response) throws Exception {
        BufferedWriter writer = response.getWriter();

        // Obter os query parameters (Projeto, Zona e Nome do Grupo)
        String projectID = "cn2026labels";
        String zone = request.getFirstQueryParameter("zone").orElse("europe-southwest1-a");
        String migName = request.getFirstQueryParameter("mig").orElse("grupo-madrid-servidores");

        List<String> ipsList = new ArrayList<>(); //lista que armazenará os ips

        //Chamar a API do compute para obter a lista de IPs desse grupo de instância
        try (InstancesClient client = InstancesClient.create()) {
            for (Instance instance : client.list(projectID, zone).iterateAll()) {

                //Verifica se a máquina está a correr e se pertence ao grupo que procuramos
                if (instance.getStatus().compareTo("RUNNING") == 0 && instance.getName().startsWith(migName)) {

                    //Extrai o IP público da VM
                    String ip = instance.getNetworkInterfaces(0).getAccessConfigs(0).getNatIP();
                    ipsList.add(ip);
                }
            }
        } catch (Exception e) {
            response.setStatusCode(500);
            writer.write("Erro ao consultar GCP: " + e.getMessage());
            return;
        }

        //Retorna a lista de IPs num objeto em formato JSON
        response.setContentType("application/json");
        Gson gson = new Gson();
        writer.write(gson.toJson(ipsList));
    }
}