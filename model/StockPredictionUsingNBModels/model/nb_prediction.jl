### A Pluto.jl notebook ###
# v0.19.27

using Markdown
using InteractiveUtils

# ╔═╡ 6eb1f50e-5b43-11ee-3cb6-65f9ddc75469
using DelimitedFiles

# ╔═╡ 329cdcbc-5f64-4cc1-a40c-012085b1a871
using Statistics

# ╔═╡ d863b8cf-253f-4319-a1f6-bd5d4dc8542e
pwd()

# ╔═╡ a534733f-30c1-4c12-8118-53a9badc4169
path = "/Users/duc/Documents/Workspace/StockPredictionUsingNBModels/dat_processing/Sheet1.txt"

# ╔═╡ c8b6add7-c582-464b-be5a-81cef6181180
function readData(path)
	A = readdlm(path,',')
	X = A[1:1207,2:10] 
	y = Int.(A[1:1207,12])
	return X,y
end

# ╔═╡ 45ac8388-aa40-4675-a7d9-d56d0be7ca46
X, y = readData(path)


# ╔═╡ 9a5eb7e1-ad21-4951-bc51-ab8812e58be4
function readTestData(path) 
	A_test = readdlm(path,',')
	X_test = A_test[1208:1509,2:10]
	y_test = Int.(A_test[1208:1509,12])
	return X_test, y_test
end

# ╔═╡ 8bf26c64-fd37-4fd8-916f-ded70e1dabb3
X_test, y_test = readTestData(path)

# ╔═╡ e944bd93-e6d7-4fa1-a61e-ac7259707768
mean(X, dims = 1) #mean of all features

# ╔═╡ 1e614ea7-c653-4f0b-8451-be7746c807be
idx_0 =  y .== 0

# ╔═╡ e96c243f-0b1e-4981-9de8-ecc3aaa7b11d
idx_1 = y .== 1

# ╔═╡ 68e2f506-398c-4cc8-b47b-2429f894febc
X_0 = X[idx_0,:]

# ╔═╡ 5a6aeca0-88b2-4a3d-9330-f4a56695b5e7
X_1 = X[idx_1,:]

# ╔═╡ 4ade32d5-8ee1-4f29-9203-20641069b25b
mean(X_0,dims = 1) #m0

# ╔═╡ a9176688-f7f6-4216-8dc4-2bd211e2e8ba
mean(X_1,dims = 1) #m1

# ╔═╡ 1b7f1e00-b9d3-4dfa-a4da-e308027f31d9
μ_0 = mean(X_0,dims = 1)

# ╔═╡ dc158717-4aa0-493b-b3da-047eded48a1f
σ_0 = std(X_0, dims = 1)

# ╔═╡ b5f3deb7-5ccf-40e9-8ba0-1e6630b47dca
function train(X,y)
	K = length(unique(y)) 
	N, D = size(X)
	μ = zeros(K, D)
	σ = zeros(K, D)
	θ = zeros(K)
	for k=1:K
		idx_k = (y .== k-1)
		X_k = X[idx_k,:]
		μ[k,:] = mean(X_k,dims = 1)
		σ[k,:] = std(X_k, dims = 1)
		θ[k] = sum(idx_k)/N
	end
	return θ , μ, σ
end

# ╔═╡ 5126fb2e-e648-4351-85d8-75283cc23089
θ,μ,σ = train(X,y)

# ╔═╡ f140e361-4081-4132-b75f-282c96fac15d
θ_o = θ[1]

# ╔═╡ 31a3849a-4ec8-453e-833c-e35437f8f0b2
log(θ_o)

# ╔═╡ 4a1853a7-7d9c-4da2-9da3-9635c0c378ca
function classify(θ, μ, σ, x)
	K,D = size(μ)
	p = zeros(K)
	for k=1:K
		for j = 1:D
			p[k] -= log(σ[k,j]) + (x[j] - μ[k,j])^2/(2*σ[k,j]^2) 
		end
		p[k] += log(θ[k])
	end
	return argmax(p) - 1
end

# ╔═╡ ba70aec9-d6de-4cd7-b5dd-bd1e8bb889fb
classify(θ,μ,σ,X)	

# ╔═╡ 29b47dbc-2734-4732-86cf-e00715efb16a
classify(θ,μ,σ,X_test)

# ╔═╡ 7aa03e15-a0b9-44e1-bf1c-0dac8406f55c
z = [classify(θ, μ, σ, X[i,:]) for i=1:length(y)]

# ╔═╡ 3729aba8-8dc9-4920-91d5-ed7fed3cd4bb
z_test = [classify(θ,μ,σ,X_test[i,:]) for i=1:length(y_test)]

# ╔═╡ 548c2231-bc84-4123-b538-77ce4de777f5
#Accuracy of train data
accuracy_train = sum(z .== y)/length(y) 

# ╔═╡ a0ae237f-65a3-4365-b0e9-4e5a57cdaefe
#Accuracy of train data
sum(z_test .== y_test)/length(y_test)

# ╔═╡ 00000000-0000-0000-0000-000000000001
PLUTO_PROJECT_TOML_CONTENTS = """
[deps]
DelimitedFiles = "8bb1440f-4735-579b-a4ab-409b98df4dab"
Statistics = "10745b16-79ce-11e8-11f9-7d13ad32a3b2"

[compat]
DelimitedFiles = "~1.9.1"
"""

# ╔═╡ 00000000-0000-0000-0000-000000000002
PLUTO_MANIFEST_TOML_CONTENTS = """
# This file is machine-generated - editing it directly is not advised

julia_version = "1.9.3"
manifest_format = "2.0"
project_hash = "93943a1a7d60b9d0c100a6f10786fee114842081"

[[deps.ArgTools]]
uuid = "0dad84c5-d112-42e6-8d28-ef12dabb789f"
version = "1.1.1"

[[deps.Artifacts]]
uuid = "56f22d72-fd6d-98f1-02f0-08ddc0907c33"

[[deps.Base64]]
uuid = "2a0f44e3-6c83-55bd-87e4-b1978d98bd5f"

[[deps.CompilerSupportLibraries_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "e66e0078-7015-5450-92f7-15fbd957f2ae"
version = "1.0.5+0"

[[deps.Dates]]
deps = ["Printf"]
uuid = "ade2ca70-3891-5945-98fb-dc099432e06a"

[[deps.DelimitedFiles]]
deps = ["Mmap"]
git-tree-sha1 = "9e2f36d3c96a820c678f2f1f1782582fcf685bae"
uuid = "8bb1440f-4735-579b-a4ab-409b98df4dab"
version = "1.9.1"

[[deps.Downloads]]
deps = ["ArgTools", "FileWatching", "LibCURL", "NetworkOptions"]
uuid = "f43a241f-c20a-4ad4-852c-f6b1247861c6"
version = "1.6.0"

[[deps.FileWatching]]
uuid = "7b1f6079-737a-58dc-b8bc-7a2ca5c1b5ee"

[[deps.InteractiveUtils]]
deps = ["Markdown"]
uuid = "b77e0a4c-d291-57a0-90e8-8db25a27a240"

[[deps.LibCURL]]
deps = ["LibCURL_jll", "MozillaCACerts_jll"]
uuid = "b27032c2-a3e7-50c8-80cd-2d36dbcbfd21"
version = "0.6.3"

[[deps.LibCURL_jll]]
deps = ["Artifacts", "LibSSH2_jll", "Libdl", "MbedTLS_jll", "Zlib_jll", "nghttp2_jll"]
uuid = "deac9b47-8bc7-5906-a0fe-35ac56dc84c0"
version = "7.84.0+0"

[[deps.LibGit2]]
deps = ["Base64", "NetworkOptions", "Printf", "SHA"]
uuid = "76f85450-5226-5b5a-8eaa-529ad045b433"

[[deps.LibSSH2_jll]]
deps = ["Artifacts", "Libdl", "MbedTLS_jll"]
uuid = "29816b5a-b9ab-546f-933c-edad1886dfa8"
version = "1.10.2+0"

[[deps.Libdl]]
uuid = "8f399da3-3557-5675-b5ff-fb832c97cbdb"

[[deps.LinearAlgebra]]
deps = ["Libdl", "OpenBLAS_jll", "libblastrampoline_jll"]
uuid = "37e2e46d-f89d-539d-b4ee-838fcccc9c8e"

[[deps.Logging]]
uuid = "56ddb016-857b-54e1-b83d-db4d58db5568"

[[deps.Markdown]]
deps = ["Base64"]
uuid = "d6f4376e-aef5-505a-96c1-9c027394607a"

[[deps.MbedTLS_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "c8ffd9c3-330d-5841-b78e-0817d7145fa1"
version = "2.28.2+0"

[[deps.Mmap]]
uuid = "a63ad114-7e13-5084-954f-fe012c677804"

[[deps.MozillaCACerts_jll]]
uuid = "14a3606d-f60d-562e-9121-12d972cd8159"
version = "2022.10.11"

[[deps.NetworkOptions]]
uuid = "ca575930-c2e3-43a9-ace4-1e988b2c1908"
version = "1.2.0"

[[deps.OpenBLAS_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "Libdl"]
uuid = "4536629a-c528-5b80-bd46-f80d51c5b363"
version = "0.3.21+4"

[[deps.Pkg]]
deps = ["Artifacts", "Dates", "Downloads", "FileWatching", "LibGit2", "Libdl", "Logging", "Markdown", "Printf", "REPL", "Random", "SHA", "Serialization", "TOML", "Tar", "UUIDs", "p7zip_jll"]
uuid = "44cfe95a-1eb2-52ea-b672-e2afdf69b78f"
version = "1.9.2"

[[deps.Printf]]
deps = ["Unicode"]
uuid = "de0858da-6303-5e67-8744-51eddeeeb8d7"

[[deps.REPL]]
deps = ["InteractiveUtils", "Markdown", "Sockets", "Unicode"]
uuid = "3fa0cd96-eef1-5676-8a61-b3b8758bbffb"

[[deps.Random]]
deps = ["SHA", "Serialization"]
uuid = "9a3f8284-a2c9-5f02-9a11-845980a1fd5c"

[[deps.SHA]]
uuid = "ea8e919c-243c-51af-8825-aaa63cd721ce"
version = "0.7.0"

[[deps.Serialization]]
uuid = "9e88b42a-f829-5b0c-bbe9-9e923198166b"

[[deps.Sockets]]
uuid = "6462fe0b-24de-5631-8697-dd941f90decc"

[[deps.SparseArrays]]
deps = ["Libdl", "LinearAlgebra", "Random", "Serialization", "SuiteSparse_jll"]
uuid = "2f01184e-e22b-5df5-ae63-d93ebab69eaf"

[[deps.Statistics]]
deps = ["LinearAlgebra", "SparseArrays"]
uuid = "10745b16-79ce-11e8-11f9-7d13ad32a3b2"
version = "1.9.0"

[[deps.SuiteSparse_jll]]
deps = ["Artifacts", "Libdl", "Pkg", "libblastrampoline_jll"]
uuid = "bea87d4a-7f5b-5778-9afe-8cc45184846c"
version = "5.10.1+6"

[[deps.TOML]]
deps = ["Dates"]
uuid = "fa267f1f-6049-4f14-aa54-33bafae1ed76"
version = "1.0.3"

[[deps.Tar]]
deps = ["ArgTools", "SHA"]
uuid = "a4e569a6-e804-4fa4-b0f3-eef7a1d5b13e"
version = "1.10.0"

[[deps.UUIDs]]
deps = ["Random", "SHA"]
uuid = "cf7118a7-6976-5b1a-9a39-7adc72f591a4"

[[deps.Unicode]]
uuid = "4ec0a83e-493e-50e2-b9ac-8f72acf5a8f5"

[[deps.Zlib_jll]]
deps = ["Libdl"]
uuid = "83775a58-1f1d-513f-b197-d71354ab007a"
version = "1.2.13+0"

[[deps.libblastrampoline_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "8e850b90-86db-534c-a0d3-1478176c7d93"
version = "5.8.0+0"

[[deps.nghttp2_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "8e850ede-7688-5339-a07c-302acd2aaf8d"
version = "1.48.0+0"

[[deps.p7zip_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "3f19e933-33d8-53b3-aaab-bd5110c3b7a0"
version = "17.4.0+0"
"""

# ╔═╡ Cell order:
# ╠═6eb1f50e-5b43-11ee-3cb6-65f9ddc75469
# ╠═d863b8cf-253f-4319-a1f6-bd5d4dc8542e
# ╠═a534733f-30c1-4c12-8118-53a9badc4169
# ╠═c8b6add7-c582-464b-be5a-81cef6181180
# ╠═45ac8388-aa40-4675-a7d9-d56d0be7ca46
# ╠═9a5eb7e1-ad21-4951-bc51-ab8812e58be4
# ╠═8bf26c64-fd37-4fd8-916f-ded70e1dabb3
# ╠═329cdcbc-5f64-4cc1-a40c-012085b1a871
# ╠═e944bd93-e6d7-4fa1-a61e-ac7259707768
# ╠═1e614ea7-c653-4f0b-8451-be7746c807be
# ╠═e96c243f-0b1e-4981-9de8-ecc3aaa7b11d
# ╠═68e2f506-398c-4cc8-b47b-2429f894febc
# ╠═5a6aeca0-88b2-4a3d-9330-f4a56695b5e7
# ╠═4ade32d5-8ee1-4f29-9203-20641069b25b
# ╠═a9176688-f7f6-4216-8dc4-2bd211e2e8ba
# ╠═1b7f1e00-b9d3-4dfa-a4da-e308027f31d9
# ╠═dc158717-4aa0-493b-b3da-047eded48a1f
# ╠═b5f3deb7-5ccf-40e9-8ba0-1e6630b47dca
# ╠═5126fb2e-e648-4351-85d8-75283cc23089
# ╠═f140e361-4081-4132-b75f-282c96fac15d
# ╠═31a3849a-4ec8-453e-833c-e35437f8f0b2
# ╠═4a1853a7-7d9c-4da2-9da3-9635c0c378ca
# ╠═ba70aec9-d6de-4cd7-b5dd-bd1e8bb889fb
# ╠═29b47dbc-2734-4732-86cf-e00715efb16a
# ╠═7aa03e15-a0b9-44e1-bf1c-0dac8406f55c
# ╠═3729aba8-8dc9-4920-91d5-ed7fed3cd4bb
# ╠═548c2231-bc84-4123-b538-77ce4de777f5
# ╠═a0ae237f-65a3-4365-b0e9-4e5a57cdaefe
# ╟─00000000-0000-0000-0000-000000000001
# ╟─00000000-0000-0000-0000-000000000002
